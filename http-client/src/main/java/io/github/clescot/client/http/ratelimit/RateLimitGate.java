package io.github.clescot.client.http.ratelimit;

import com.google.common.collect.Maps;
import io.github.clescot.core.http.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static io.github.clescot.client.Constants.*;
import static io.github.clescot.client.RequestClient.STATIC_SCOPE;
import static io.github.clescot.client.http.ratelimit.RateLimitHeaders.ServiceLimit;
import static io.github.clescot.client.http.ratelimit.RateLimitHeaders.Snapshot;

/**
 * The wait-on-exhaustion gate for the IETF RateLimit response header fields.
 *
 * <p>When a response advertises an exhausted service limit ({@code r=0} on a quota unit that
 * counts request admissions), the gate remembers "exhausted until" for the target authority.
 * Before the next call to that authority, {@link #awaitIfExhausted(String)} waits until the
 * advertised window (plus a little jitter) has elapsed. Newer exhausted signals may extend the
 * deadline, never shorten it; a healthy quota does not cancel an active pause.
 *
 * <p>Waits longer than {@code rate.limit.headers.max.threshold.in.sec} are not waited inline:
 * {@link #exhaustedWaitSeconds(HttpExchange)} reports them so that the circuit breaker opens
 * instead, exactly like an over-threshold {@code Retry-After}.
 */
public class RateLimitGate {
    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitGate.class);

    /** timing key recorded on the exchange when the gate waited before a call. */
    public static final String RATE_LIMIT_WAITING_TIME_TIMING_KEY = "rateLimitWaitingTime";
    /** upper bound of the random jitter added to a pause, in milliseconds. */
    public static final long MAX_JITTER_IN_MS = 500L;
    private static final Map<String, RateLimitGate> SHARED_GATES = Maps.newHashMap();

    /** how the gate sleeps; tests substitute it. */
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final String configurationId;
    private final boolean enabled;
    private final long maxDurationSeconds;
    private final long defaultDelaySeconds;
    private final Clock clock;
    private final Sleeper sleeper;
    private final Random jitter;
    private final Map<String, Instant> exhaustedUntilByAuthority = new ConcurrentHashMap<>();

    private RateLimitGate(
            String configurationId,
            boolean enabled,
            long maxDurationSeconds,
            long defaultDelaySeconds,
            Clock clock,
            Sleeper sleeper,
            Random jitter) {
        this.configurationId = configurationId;
        this.enabled = enabled;
        this.maxDurationSeconds = maxDurationSeconds;
        this.defaultDelaySeconds = defaultDelaySeconds;
        this.clock = clock;
        this.sleeper = sleeper;
        this.jitter = jitter;
    }

    /**
     * builds the gate for a configuration, honouring {@code rate.limit.headers.scope}: an
     * {@code instance} gate belongs to the calling task, a {@code static} gate is shared in the
     * JVM per configuration id (first settings win, like the local rate limiter).
     */
    public static RateLimitGate fromSettings(String configurationId, Map<String, String> settings) {
        return fromSettings(configurationId, settings, Clock.systemUTC(), Thread::sleep, new Random());
    }

    /**
     * the settings-driven factory with the clock, sleeper and jitter seams exposed, so that a
     * caller whose HTTP loop it does not own (the SSE source) can test its pacing against a fake
     * clock.
     */
    public static RateLimitGate fromSettings(
            String configurationId,
            Map<String, String> settings,
            Clock clock,
            Sleeper sleeper,
            Random jitter) {
        Map<String, String> effectiveSettings = settings == null ? Maps.newHashMap() : settings;
        boolean enabled = Boolean.parseBoolean(effectiveSettings.getOrDefault(RATE_LIMIT_HEADERS_ENABLED, DEFAULT_RATE_LIMIT_HEADERS_ENABLED_VALUE));
        String scope = effectiveSettings.getOrDefault(RATE_LIMIT_HEADERS_SCOPE, DEFAULT_RATE_LIMIT_HEADERS_SCOPE_VALUE);
        long maxDuration =
                Long.parseLong(effectiveSettings.getOrDefault(RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC, DEFAULT_RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC));
        long defaultDelay =
                Long.parseLong(effectiveSettings.getOrDefault(RATE_LIMIT_HEADERS_DEFAULT_DELAY_IN_SEC, DEFAULT_RATE_LIMIT_HEADERS_DEFAULT_DELAY_IN_SEC));
        if (STATIC_SCOPE.equalsIgnoreCase(scope)) {
            synchronized (SHARED_GATES) {
                return SHARED_GATES.computeIfAbsent(
                        configurationId,
                        id -> new RateLimitGate(id, enabled, maxDuration, defaultDelay, clock, sleeper, jitter));
            }
        }
        return new RateLimitGate(configurationId, enabled, maxDuration, defaultDelay, clock, sleeper, jitter);
    }

    /** a gate that never waits: the behaviour of a configuration built without one. */
    public static RateLimitGate disabled(String configurationId) {
        return new RateLimitGate(
                configurationId,
                false,
                Long.parseLong(DEFAULT_RATE_LIMIT_HEADERS_MAX_DURATION_IN_SEC),
                Long.parseLong(DEFAULT_RATE_LIMIT_HEADERS_DEFAULT_DELAY_IN_SEC),
                Clock.systemUTC(),
                Thread::sleep,
                new Random());
    }

    /** for tests: forgets the shared {@code static} gates. */
    public static void clearSharedGates() {
        synchronized (SHARED_GATES) {
            SHARED_GATES.clear();
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * waits until the exhausted window advertised for the request's target authority has
     * elapsed, if any.
     *
     * @param url the request URL
     * @return the milliseconds waited (0 when there was nothing to wait for)
     */
    public long awaitIfExhausted(String url) {
        if (!enabled || url == null) {
            return 0L;
        }
        String authority = authorityOf(url);
        if (authority == null) {
            return 0L;
        }
        Instant deadline = exhaustedUntilByAuthority.get(authority);
        if (deadline == null) {
            return 0L;
        }
        long remainingMs = Duration.between(clock.instant(), deadline).toMillis();
        if (remainingMs <= 0) {
            exhaustedUntilByAuthority.remove(authority, deadline);
            return 0L;
        }
        LOGGER.info(
                "configuration '{}' : waiting '{}' ms for the advertised rate limit window of '{}' to elapse",
                configurationId,
                remainingMs,
                authority);
        try {
            sleeper.sleep(remainingMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the advertised rate limit window", e);
        }
        exhaustedUntilByAuthority.remove(authority, deadline);
        return remainingMs;
    }

    /**
     * records the service limits a response advertises, extending the pause of the target
     * authority when a quota is exhausted.
     */
    public void observe(HttpExchange exchange) {
        if (!enabled || exchange == null || exchange.getResponse() == null || exchange.getRequest() == null) {
            return;
        }
        observe(exchange.getRequest().getUrl(), exchange.getResponse().getHeaders());
    }

    /**
     * records the service limits a response advertises, extending the pause of the target
     * authority when a quota is exhausted. Takes the response headers directly, for clients that
     * do not build an {@link HttpExchange} (the SSE source, whose HTTP exchanges belong to
     * launchdarkly-eventsource).
     *
     * @param url     the request URL: only its authority keys the pause
     * @param headers the response headers
     */
    public void observe(String url, Map<String, List<String>> headers) {
        if (!enabled || url == null || headers == null) {
            return;
        }
        String authority = authorityOf(url);
        if (authority == null) {
            return;
        }
        Snapshot snapshot = RateLimitHeaders.parse(headers);
        long waitSeconds = exhaustedWaitSeconds(snapshot);
        if (waitSeconds <= 0) {
            // a healthy quota does not cancel an active pause: the deadline is never shortened.
            return;
        }
        Instant candidate = clock.instant().plusSeconds(waitSeconds).plusMillis(jitterMillis(waitSeconds));
        exhaustedUntilByAuthority.merge(authority, candidate, (existing, fresh) -> existing.isAfter(fresh) ? existing : fresh);
        LOGGER.info(
                "configuration '{}' : advertised rate limit exhausted for '{}', pausing calls until '{}'",
                configurationId,
                authority,
                candidate);
    }

    /**
     * the pause the advertised limits still require for the url's authority, in milliseconds,
     * without sleeping. Zero when the gate is disabled, nothing was advertised, or the window has
     * already elapsed.
     *
     * <p>Unlike {@link #awaitIfExhausted(String)}, reading the remaining pause does not consume
     * the deadline: the deadline is an absolute instant, so it expires on its own, and the caller
     * decides when (and how) to wait.
     *
     * @param url the request URL
     * @return the remaining pause in milliseconds, 0 when there is nothing to wait for
     */
    public long remainingPauseMillis(String url) {
        if (!enabled || url == null) {
            return 0L;
        }
        String authority = authorityOf(url);
        if (authority == null) {
            return 0L;
        }
        Instant deadline = exhaustedUntilByAuthority.get(authority);
        if (deadline == null) {
            return 0L;
        }
        long remainingMs = Duration.between(clock.instant(), deadline).toMillis();
        if (remainingMs <= 0) {
            exhaustedUntilByAuthority.remove(authority, deadline);
            return 0L;
        }
        return remainingMs;
    }

    /**
     * the exhausted window (in seconds, capped) a response advertises, 0 when no request
     * admission quota is exhausted. Used by the circuit-breaker decision.
     */
    public long exhaustedWaitSeconds(HttpExchange exchange) {
        if (!enabled || exchange == null || exchange.getResponse() == null) {
            return 0L;
        }
        return exhaustedWaitSeconds(RateLimitHeaders.parse(exchange.getResponse().getHeaders()));
    }

    long exhaustedWaitSeconds(Snapshot snapshot) {
        long waitSeconds = 0L;
        for (ServiceLimit serviceLimit : snapshot.serviceLimits()) {
            if (serviceLimit.remaining() == 0 && snapshot.countsRequestAdmissions(serviceLimit)) {
                long candidate = snapshot.advertisedWindowSeconds(serviceLimit).orElse(defaultDelaySeconds);
                // an explicit "t=0" means the window has already elapsed
                if (candidate > 0) {
                    waitSeconds = Math.max(waitSeconds, Math.min(candidate, maxDurationSeconds));
                }
            }
        }
        return waitSeconds;
    }

    private long jitterMillis(long waitSeconds) {
        long max = Math.min(waitSeconds * 1000L, MAX_JITTER_IN_MS);
        return max <= 0 ? 0L : jitter.nextLong(max + 1);
    }

    /**
     * the scheme://host:port key a pause is remembered under, or null when the url carries no
     * host. Two connectors observing the same authority must normalise it the same way.
     */
    public static String authorityOf(String url) {
        if (url == null) {
            return null;
        }
        try {
            URI uri = URI.create(url);
            String host = uri.getHost();
            if (host == null) {
                return null;
            }
            String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(java.util.Locale.ROOT);
            int port = uri.getPort();
            if (port == -1) {
                port = "https".equals(scheme) ? 443 : 80;
            }
            return scheme + "://" + host.toLowerCase(java.util.Locale.ROOT) + ":" + port;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
