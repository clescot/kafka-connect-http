package io.github.clescot.client.http.ratelimit;

import static io.github.clescot.client.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Maps;
import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RateLimitGateTest {

  private final MutableClock clock = new MutableClock(Instant.parse("2026-10-26T00:00:00Z"));
  private final RecordingSleeper sleeper = new RecordingSleeper();

  @AfterEach
  void tearDown() {
    RateLimitGate.clearSharedGates();
  }

  private RateLimitGate gate(Map<String, String> settings) {
    return RateLimitGate.fromSettings("test-config", settings, clock, sleeper, new Random(42L));
  }

  private static Map<String, String> settings(String... keyValues) {
    Map<String, String> settings = Maps.newHashMap();
    for (int i = 0; i < keyValues.length; i += 2) {
      settings.put(keyValues[i], keyValues[i + 1]);
    }
    return settings;
  }

  private static HttpExchange exchange(String url, String... headerKeyValues) {
    HttpResponse response = new HttpResponse(200, "OK");
    Map<String, List<String>> headers = Maps.newHashMap();
    for (int i = 0; i < headerKeyValues.length; i += 2) {
      headers.put(headerKeyValues[i], List.of(headerKeyValues[i + 1]));
    }
    response.setHeaders(headers);
    HttpRequest request = new HttpRequest(url, HttpRequest.Method.GET);
    return HttpExchange.Builder.anHttpExchange()
        .withHttpRequest(request)
        .withHttpResponse(response)
        .withDuration(1L)
        .at(OffsetDateTime.now(ZoneOffset.UTC))
        .withAttempts(new AtomicInteger(1))
        .withAttributes(Maps.newHashMap())
        .withTimings(Maps.newHashMap())
        .build();
  }

  @Nested
  class WaitOnExhaustion {

    @Test
    void test_waits_for_the_advertised_effective_window() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=30"));
      long waited = gate.awaitIfExhausted("http://api.example.com/stuff");
      assertThat(waited).isBetween(30_000L, 30_500L).isEqualTo(sleeper.sleptMillis());
    }

    @Test
    void test_no_wait_when_quota_remains() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=5;t=30"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff")).isZero();
    }

    @Test
    void test_waits_until_window_elapses_then_stops() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=5"));
      clock.advanceSeconds(6);
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff")).isZero();
    }

    @Test
    void test_deadline_is_never_shortened() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=30"));
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=5"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff"))
          .isGreaterThanOrEqualTo(30_000L);
    }

    @Test
    void test_deadline_is_extended_by_new_signals() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=5"));
      clock.advanceSeconds(1);
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=30"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff"))
          .isBetween(30_000L, 30_500L);
    }

    @Test
    void test_healthy_quota_does_not_cancel_an_active_pause() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=30"));
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=100;t=1"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff"))
          .isGreaterThanOrEqualTo(30_000L);
    }

    @Test
    void test_pause_is_keyed_by_authority() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=30"));
      assertThat(gate.awaitIfExhausted("http://other.example.com/stuff")).isZero();
      assertThat(gate.awaitIfExhausted("http://api.example.com:80/other")).isPositive();
    }

    @Test
    void test_policy_window_is_the_fallback_when_t_is_absent() {
      RateLimitGate gate = gate(settings());
      gate.observe(
          exchange(
              "http://api.example.com/stuff",
              "RateLimit-Policy", "\"daily\";q=1000;w=20",
              "RateLimit", "\"daily\";r=0"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff"))
          .isBetween(20_000L, 20_500L);
    }

    @Test
    void test_default_delay_is_the_last_resort() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff"))
          .isBetween(3_600_000L, 3_600_500L);
    }

    @Test
    void test_advertised_window_is_capped_by_max_duration() {
      RateLimitGate gate = gate(settings(RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC, "10"));
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=600"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff")).isBetween(10_000L, 10_500L);
    }

    @Test
    void test_only_request_admission_units_pause() {
      RateLimitGate gate = gate(settings());
      gate.observe(
          exchange(
              "http://api.example.com/stuff",
              "RateLimit-Policy", "\"bytes\";q=10;qu=\"content-bytes\";w=30",
              "RateLimit", "\"bytes\";r=0;t=30"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff")).isZero();

      gate.observe(
          exchange(
              "http://api.example.com/stuff",
              "RateLimit-Policy", "\"slots\";q=1;qu=\"concurrent-requests\";w=30",
              "RateLimit", "\"slots\";r=0;t=30"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff")).isPositive();
    }

    @Test
    void test_jitter_is_bounded_by_the_window_and_the_maximum() {
      RateLimitGate gate = gate(settings());
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=1"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff")).isBetween(1_000L, 1_500L);
    }

    @Test
    void test_disabled_gate_never_waits() {
      RateLimitGate gate = gate(settings(RATE_LIMIT_HEADERS_ENABLED, "false"));
      gate.observe(exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=30"));
      assertThat(gate.awaitIfExhausted("http://api.example.com/stuff")).isZero();
    }
  }

  @Nested
  class HeaderMapObservation {

    @Test
    void test_headers_can_be_observed_without_an_exchange() {
      RateLimitGate gate = gate(settings());
      gate.observe("http://api.example.com/stuff", Map.of("ratelimit", List.of("\"default\";r=0;t=30")));
      assertThat(gate.remainingPauseMillis("http://api.example.com/stuff"))
          .isBetween(30_000L, 30_500L);
    }

    @Test
    void test_remaining_pause_does_not_sleep_nor_consume_the_deadline() {
      RateLimitGate gate = gate(settings());
      gate.observe("http://api.example.com/stuff", Map.of("RateLimit", List.of("\"default\";r=0;t=30")));
      clock.advanceSeconds(2);
      assertThat(gate.remainingPauseMillis("http://api.example.com/stuff"))
          .isBetween(28_000L, 28_500L);
      assertThat(gate.remainingPauseMillis("http://api.example.com/stuff")).isPositive();
      assertThat(sleeper.sleptMillis()).isZero();
    }

    @Test
    void test_remaining_pause_is_zero_once_the_window_elapsed() {
      RateLimitGate gate = gate(settings());
      gate.observe("http://api.example.com/stuff", Map.of("RateLimit", List.of("\"default\";r=0;t=5")));
      clock.advanceSeconds(6);
      assertThat(gate.remainingPauseMillis("http://api.example.com/stuff")).isZero();
    }

    @Test
    void test_remaining_pause_is_zero_when_disabled() {
      RateLimitGate gate = gate(settings(RATE_LIMIT_HEADERS_ENABLED, "false"));
      gate.observe("http://api.example.com/stuff", Map.of("RateLimit", List.of("\"default\";r=0;t=30")));
      assertThat(gate.remainingPauseMillis("http://api.example.com/stuff")).isZero();
    }
  }

  @Nested
  class Decision {

    @Test
    void test_exhausted_wait_seconds_are_capped() {
      RateLimitGate gate = gate(settings(RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC, "10"));
      assertThat(
              gate.exhaustedWaitSeconds(
                  exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=600")))
          .isEqualTo(10L);
    }

    @Test
    void test_exhausted_wait_seconds_are_zero_when_quota_remains() {
      RateLimitGate gate = gate(settings());
      assertThat(
              gate.exhaustedWaitSeconds(
                  exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=9;t=600")))
          .isZero();
    }

    @Test
    void test_explicit_zero_window_means_no_pause() {
      RateLimitGate gate = gate(settings());
      assertThat(
              gate.exhaustedWaitSeconds(
                  exchange("http://api.example.com/stuff", "RateLimit", "\"default\";r=0;t=0")))
          .isZero();
    }
  }

  @Nested
  class Scope {

    @Test
    void test_static_scope_shares_one_gate_per_configuration_id() {
      Map<String, String> settings = settings(RATE_LIMIT_HEADERS_SCOPE, "static");
      RateLimitGate first =
          RateLimitGate.fromSettings("shared", settings, clock, sleeper, new Random(1));
      RateLimitGate second =
          RateLimitGate.fromSettings("shared", settings, clock, sleeper, new Random(2));
      assertThat(first).isSameAs(second);
    }

    @Test
    void test_instance_scope_builds_a_gate_per_call() {
      Map<String, String> settings = settings(RATE_LIMIT_HEADERS_SCOPE, "instance");
      assertThat(RateLimitGate.fromSettings("isolated", settings, clock, sleeper, new Random(1)))
          .isNotSameAs(
              RateLimitGate.fromSettings("isolated", settings, clock, sleeper, new Random(2)));
    }
  }

  static class MutableClock extends Clock {
    private Instant instant;

    MutableClock(Instant instant) {
      this.instant = instant;
    }

    void advanceSeconds(long seconds) {
      instant = instant.plusSeconds(seconds);
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

  static class RecordingSleeper implements RateLimitGate.Sleeper {
    private long sleptMillis;

    @Override
    public void sleep(long millis) {
      sleptMillis += millis;
    }

    long sleptMillis() {
      return sleptMillis;
    }
  }
}
