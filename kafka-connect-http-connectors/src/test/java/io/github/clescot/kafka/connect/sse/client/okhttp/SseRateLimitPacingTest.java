package io.github.clescot.kafka.connect.sse.client.okhttp;

import static io.github.clescot.client.Constants.DEFAULT_RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC;
import static io.github.clescot.client.Constants.RATE_LIMIT_HEADERS_ENABLED;
import static io.github.clescot.client.Constants.RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.ResponseHeaders;
import com.launchdarkly.eventsource.RetryDelayStrategy;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.StreamHttpErrorException;
import com.launchdarkly.logging.LDLogger;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class SseRateLimitPacingTest {

  private static final URI STREAM_URI = URI.create("https://events.example.com/stream");
  private static final long ONE_SECOND = 1000L;

  private final MutableClock clock = new MutableClock(Instant.parse("2026-10-26T00:00:00Z"));

  private SseRateLimitPacing pacing(Map<String, String> settings) {
    // the pacing never sleeps itself: the library owns the wait, so the sleeper is a no-op
    return SseRateLimitPacing.fromSettings(
        "test-config", STREAM_URI, settings, clock, millis -> {}, new Random(42L));
  }

  private void attempt(SseRateLimitPacing pacing, int statusCode, ResponseHeaders headers) {
    try {
      pacing.connectStrategy(connectStrategy(statusCode, headers)).createClient(null).connect(null);
    } catch (StreamException e) {
      // launchdarkly turns a failed attempt into a reconnect; only the observation matters here
    }
  }

  private RetryDelayStrategy paced(SseRateLimitPacing pacing, long delegateDelayMillis) {
    return pacing.retryDelayStrategy(fixed(delegateDelayMillis, null));
  }

  @Test
  void without_an_advertised_delay_the_library_delay_is_kept() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());

    attempt(pacing, 200, headers());

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isEqualTo(ONE_SECOND);
  }

  @Test
  void an_exhausted_rate_limit_window_pauses_the_next_attempt() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());

    attempt(pacing, 200, headers("RateLimit", "\"default\";r=0;t=5"));

    // jitter can extend the pause by up to 500 ms
    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isBetween(5 * ONE_SECOND, 5 * ONE_SECOND + 500L);
  }

  @Test
  void retry_after_wins_over_an_exhausted_rate_limit_window() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());

    attempt(pacing, 429, headers("Retry-After", "3", "RateLimit", "\"default\";r=0;t=30"));

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isEqualTo(3 * ONE_SECOND);
  }

  @Test
  void retry_after_on_a_status_the_configuration_ignores_falls_back_to_the_rate_limit_window()
      {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());

    attempt(pacing, 500, headers("Retry-After", "3", "RateLimit", "\"default\";r=0;t=5"));

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isBetween(5 * ONE_SECOND, 5 * ONE_SECOND + 500L);
  }

  @Test
  void an_advertised_delay_never_shortens_the_library_backoff() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());

    attempt(pacing, 429, headers("Retry-After", "3"));

    assertThat(paced(pacing, 30 * ONE_SECOND).apply(30 * ONE_SECOND).getDelayMillis())
        .isEqualTo(30 * ONE_SECOND);
  }

  @Test
  void an_elapsed_window_returns_to_the_library_delay() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());
    attempt(pacing, 200, headers("RateLimit", "\"default\";r=0;t=5"));

    clock.advance(6 * ONE_SECOND);

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isEqualTo(ONE_SECOND);
  }

  @Test
  void the_next_strategy_returned_by_the_backoff_is_paced_too() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());
    attempt(pacing, 200, headers("RateLimit", "\"default\";r=0;t=5"));
    RetryDelayStrategy unwrappedNext = fixed(ONE_SECOND, null);

    RetryDelayStrategy.Result first =
        pacing.retryDelayStrategy(fixed(ONE_SECOND, unwrappedNext)).apply(ONE_SECOND);

    assertThat(first.getNext()).isNotNull().isNotSameAs(unwrappedNext);
    assertThat(first.getNext().apply(ONE_SECOND).getDelayMillis())
        .isGreaterThanOrEqualTo(5 * ONE_SECOND);
  }

  @Test
  void the_default_delay_applies_when_the_window_is_absent() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());

    attempt(pacing, 200, headers("RateLimit", "\"default\";r=0"));

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isBetween(3600 * ONE_SECOND, 3600 * ONE_SECOND + 500L);
  }

  @Test
  void a_window_longer_than_the_max_duration_is_capped() {
    SseRateLimitPacing pacing =
        pacing(
            Map.of(
                RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC,
                DEFAULT_RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC));

    attempt(pacing, 200, headers("RateLimit", "\"default\";r=0;t=172800"));

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isBetween(
            86400 * ONE_SECOND,
            86400 * ONE_SECOND + 500L);
  }

  @Test
  void a_disabled_gate_restores_the_library_delay() {
    SseRateLimitPacing pacing = pacing(Map.of(RATE_LIMIT_HEADERS_ENABLED, "false"));

    attempt(pacing, 429, headers("Retry-After", "3", "RateLimit", "\"default\";r=0;t=30"));

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isEqualTo(ONE_SECOND);
  }

  @Test
  void a_date_form_retry_after_is_understood() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());
    String date =
        DateTimeFormatter.RFC_1123_DATE_TIME.format(
            ZonedDateTime.ofInstant(
                clock.instant().plusSeconds(60), ZoneOffset.UTC));

    attempt(pacing, 503, headers("Retry-After", date));

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isEqualTo(60 * ONE_SECOND);
  }

  @Test
  void an_unparsable_retry_after_falls_back_to_the_library_delay() {
    SseRateLimitPacing pacing = pacing(Maps.newHashMap());

    attempt(pacing, 429, headers("Retry-After", "not-a-date"));

    assertThat(paced(pacing, ONE_SECOND).apply(ONE_SECOND).getDelayMillis())
        .isEqualTo(ONE_SECOND);
  }

  private static ResponseHeaders headers(String... headerKeyValues) {
    List<ResponseHeaders.Header> headers = Lists.newArrayList();
    for (int i = 0; i < headerKeyValues.length; i += 2) {
      headers.add(new ResponseHeaders.Header(headerKeyValues[i], headerKeyValues[i + 1]));
    }
    return new FakeResponseHeaders(headers);
  }

  private static ConnectStrategy connectStrategy(int statusCode, ResponseHeaders headers) {
    return new ConnectStrategy() {
      @Override
      public Client createClient(LDLogger logger) {
        return new Client() {
          @Override
          public Result connect(String lastEventId) throws StreamException {
            if (statusCode >= 400) {
              throw new StreamHttpErrorException(statusCode, headers);
            }
            return new Result(new ByteArrayInputStream(new byte[0]), STREAM_URI, null, headers);
          }

          @Override
          public boolean awaitClosed(long timeoutMillis) {
            return true;
          }

          @Override
          public URI getOrigin() {
            return STREAM_URI;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  private static RetryDelayStrategy fixed(long delayMillis, RetryDelayStrategy next) {
    return new RetryDelayStrategy() {
      @Override
      public Result apply(long baseDelayMillis) {
        return new Result(delayMillis, next);
      }
    };
  }

  private static final class FakeResponseHeaders implements ResponseHeaders {
    private final List<Header> headers;

    private FakeResponseHeaders(List<Header> headers) {
      this.headers = headers;
    }

    @Override
    public int size() {
      return headers.size();
    }

    @Override
    public Header get(int index) {
      return headers.get(index);
    }

    @Override
    public String value(String name) {
      return headers.stream()
          .filter(header -> header.getName().equalsIgnoreCase(name))
          .map(Header::getValue)
          .findFirst()
          .orElse(null);
    }

    @Override
    public boolean isEmpty() {
      return headers.isEmpty();
    }
  }

  private static final class MutableClock extends Clock {
    private Instant instant;

    private MutableClock(Instant instant) {
      this.instant = instant;
    }

    private void advance(long millis) {
      instant = instant.plusMillis(millis);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instant;
    }
  }
}
