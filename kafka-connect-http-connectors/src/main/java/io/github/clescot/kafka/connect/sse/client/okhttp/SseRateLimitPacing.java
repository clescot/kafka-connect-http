package io.github.clescot.kafka.connect.sse.client.okhttp;

import static io.github.clescot.client.Constants.CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER;
import static io.github.clescot.client.Constants.DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER;
import static io.github.clescot.client.Constants.DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC;
import static io.github.clescot.client.Constants.RETRY_AFTER_MAX_DURATION_IN_SEC;
import static io.github.clescot.core.http.HttpResponse.RETRY_AFTER;
import static io.github.clescot.core.http.HttpResponse.X_RETRY_AFTER;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.ResponseHeaders;
import com.launchdarkly.eventsource.RetryDelayStrategy;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.StreamHttpErrorException;
import com.launchdarkly.logging.LDLogger;
import io.github.clescot.client.http.ratelimit.RateLimitGate;
import io.github.clescot.core.http.RetryAfterParser;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Paces the SSE source's connection attempts with the delays the server advertises.
 *
 * <p>launchdarkly-eventsource owns the connect/reconnect loop, so the pacing is inserted through
 * its two seams rather than around them: the {@link ConnectStrategy} wrapper observes the response
 * headers of every attempt (successful connections through {@link
 * ConnectStrategy.Client.Result#getHeaders()}, HTTP errors through {@link
 * StreamHttpErrorException#getHeaders()}), and the {@link RetryDelayStrategy} wrapper lengthens the
 * delay the library computed when the server advertised an exhausted IETF {@code RateLimit} window
 * or a {@code Retry-After}.
 *
 * <p>The delay stays inside the library's own interruptible sleep, so {@code stop()} still wakes
 * the stream up instead of waiting for a long advertised window to elapse.
 *
 * <p>Semantics shared with the HTTP sink:
 * <ul>
 *   <li>only an exhausted admission quota ({@code r=0} on {@code requests} or {@code
 *       concurrent-requests}) pauses, and only for the target authority;
 *   <li>{@code Retry-After} / {@code X-Retry-After} wins when present on a status code the
 *       configuration honours, and the {@code RateLimit} field is then ignored;
 *   <li>a newer exhausted signal extends the pause, a healthy quota does not cancel it;
 *   <li>the advertised delay is never shorter than the library's own backoff.
 * </ul>
 */
final class SseRateLimitPacing {
  private static final Logger LOGGER = LoggerFactory.getLogger(SseRateLimitPacing.class);

  private final String configurationId;
  private final URI uri;
  private final RateLimitGate gate;
  private final Pattern retryAfterStatusCodes;
  private final long retryAfterMaxDurationSeconds;
  private final Clock clock;
  private final String authority;
  private final AtomicReference<Instant> retryAfterDeadline = new AtomicReference<>();

  private SseRateLimitPacing(
      String configurationId,
      URI uri,
      RateLimitGate gate,
      Pattern retryAfterStatusCodes,
      long retryAfterMaxDurationSeconds,
      Clock clock) {
    this.configurationId = configurationId;
    this.uri = uri;
    this.gate = gate;
    this.retryAfterStatusCodes = retryAfterStatusCodes;
    this.retryAfterMaxDurationSeconds = retryAfterMaxDurationSeconds;
    this.clock = clock;
    this.authority = authorityOf(uri);
  }

  static SseRateLimitPacing fromSettings(
      String configurationId, URI uri, Map<String, String> settings) {
    return fromSettings(
        configurationId, uri, settings, Clock.systemUTC(), Thread::sleep, new Random());
  }

  static SseRateLimitPacing fromSettings(
      String configurationId,
      URI uri,
      Map<String, String> settings,
      Clock clock,
      RateLimitGate.Sleeper sleeper,
      Random jitter) {
    Map<String, String> effectiveSettings = settings == null ? Maps.newHashMap() : settings;
    String statusCodes =
        effectiveSettings.getOrDefault(
            CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER,
            DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER);
    long maxRetryAfter =
        Long.parseLong(
            effectiveSettings.getOrDefault(
                RETRY_AFTER_MAX_DURATION_IN_SEC, DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC));
    return new SseRateLimitPacing(
        configurationId,
        uri,
        RateLimitGate.fromSettings(configurationId, effectiveSettings, clock, sleeper, jitter),
        Pattern.compile(statusCodes),
        maxRetryAfter,
        clock);
  }

  /** wraps the connect strategy so every attempt's response headers are observed once. */
  ConnectStrategy connectStrategy(ConnectStrategy delegate) {
    return new ObservingConnectStrategy(delegate);
  }

  /** wraps the retry delay strategy so an advertised delay is never undercut. */
  RetryDelayStrategy retryDelayStrategy(RetryDelayStrategy delegate) {
    return new PacedRetryDelayStrategy(delegate);
  }

  private static String authorityOf(URI uri) {
    String authority = RateLimitGate.authorityOf(uri.toString());
    return authority == null ? uri.toString() : authority;
  }

  private static Map<String, List<String>> toMap(ResponseHeaders headers) {
    Map<String, List<String>> headerMap = Maps.newHashMap();
    for (int i = 0; i < headers.size(); i++) {
      ResponseHeaders.Header header = headers.get(i);
      headerMap.computeIfAbsent(header.getName(), name -> Lists.newArrayList()).add(header.getValue());
    }
    return headerMap;
  }

  private static String firstValue(Map<String, List<String>> headers, String... names) {
    for (String name : names) {
      for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
        if (entry.getKey() != null
            && entry.getKey().equalsIgnoreCase(name)
            && entry.getValue() != null
            && !entry.getValue().isEmpty()) {
          return entry.getValue().get(0);
        }
      }
    }
    return null;
  }

  private final class ObservingConnectStrategy extends ConnectStrategy {
    private final ConnectStrategy delegate;

    private ObservingConnectStrategy(ConnectStrategy delegate) {
      this.delegate = delegate;
    }

    private void observe(ResponseHeaders headers, Integer statusCode) {
      if (!gate.isEnabled() || headers == null) {
        return;
      }
      Map<String, List<String>> headerMap = toMap(headers);
      String retryAfter = firstValue(headerMap, RETRY_AFTER, X_RETRY_AFTER);
      if (retryAfter != null
          && statusCode != null
          && retryAfterStatusCodes.matcher(statusCode.toString()).matches()) {
        // Retry-After wins: acting on both would wait for the longer, non-authoritative window
        long advertisedSeconds =
            Math.max(
                0L,
                Math.min(
                    RetryAfterParser.secondsToWait(retryAfter, clock),
                    retryAfterMaxDurationSeconds));
        retryAfterDeadline.set(clock.instant().plusSeconds(advertisedSeconds));
        LOGGER.info(
            "configuration '{}' : the SSE server at '{}' asked to wait '{}' s before reconnecting",
            configurationId,
            authority,
            advertisedSeconds);
        return;
      }
      gate.observe(uri.toString(), headerMap);
    }

    @Override
    public Client createClient(LDLogger logger) {
      Client client = delegate.createClient(logger);
      return new Client() {
        @Override
        public Result connect(String lastEventId) throws StreamException {
          try {
            Result result = client.connect(lastEventId);
            observe(result.getHeaders(), null);
            return result;
          } catch (StreamHttpErrorException e) {
            observe(e.getHeaders(), e.getCode());
            throw e;
          }
        }

        @Override
        public boolean awaitClosed(long timeoutMillis) throws InterruptedException {
          return client.awaitClosed(timeoutMillis);
        }

        @Override
        public URI getOrigin() {
          return client.getOrigin();
        }

        @Override
        public void close() throws IOException {
          client.close();
        }
      };
    }
  }

  private final class PacedRetryDelayStrategy extends RetryDelayStrategy {
    private final RetryDelayStrategy delegate;

    private PacedRetryDelayStrategy(RetryDelayStrategy delegate) {
      this.delegate = delegate;
    }

    private long retryAfterRemainingMillis() {
      Instant deadline = retryAfterDeadline.get();
      if (deadline == null) {
        return 0L;
      }
      long remainingMs = Duration.between(clock.instant(), deadline).toMillis();
      if (remainingMs <= 0) {
        retryAfterDeadline.compareAndSet(deadline, null);
        return 0L;
      }
      return remainingMs;
    }

    @Override
    public long getDelayMillis() {
      long libraryMillis = delegate.getDelayMillis();
      long rateLimitMillis = gate.remainingPauseMillis(uri.toString());
      long retryAfterMillis = retryAfterRemainingMillis();
      long advertisedMillis = Math.max(rateLimitMillis, retryAfterMillis);
      long delayMillis = Math.max(libraryMillis, advertisedMillis);
      if (advertisedMillis > libraryMillis) {
        LOGGER.info(
            "configuration '{}' : delaying the next SSE connection attempt to '{}' by '{}' ms for the advertised {}",
            configurationId,
            authority,
            advertisedMillis,
            retryAfterMillis > rateLimitMillis ? "Retry-After" : "rate limit window");
      }
      return delayMillis;
    }

    @Override
    public RetryDelayStrategy getNext() {
      // the underlying backoff returns a new strategy per attempt; re-wrap it or the pacing
      // silently stops applying after the second retry
      RetryDelayStrategy next = delegate.getNext();
      return next == null ? null : new PacedRetryDelayStrategy(next);
    }

    @Override
    public RetryDelayStrategy withBaseDelayMillis(long millis) {
      // a server-directed `retry:` field resets the library's base delay; re-wrap the fresh
      // strategy so the reset is kept and the pacing stays in front of it
      return new PacedRetryDelayStrategy(delegate.withBaseDelayMillis(millis));
    }
  }
}
