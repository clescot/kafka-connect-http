package io.github.clescot.kafka.connect;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import dev.failsafe.RateLimiterConfig;
import io.github.clescot.kafka.connect.http.client.RetryException;
import io.github.clescot.kafka.connect.http.core.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.RATE_LIMITER_SCOPE;

/**
 *  A client interface that handles requests and native requests.
 * @param <R> request
 * @param <NR> native request
 */
@SuppressWarnings("java:S119")
public interface RequestClient<R extends Request,NR,E> extends Client<E>{
    String STATIC_SCOPE = "static";
    Map<String, RateLimiter> SHARED_RATE_LIMITERS = Maps.newHashMap();
    Logger LOGGER = LoggerFactory.getLogger(RequestClient.class);
    int ONE_REQUEST = 1;
    /**
     * convert an Request into a native request.
     *
     * @param request to build.
     * @return native request.
     */
    NR buildNativeRequest(R request);


    R buildRequest(NR nativeRequest);

    /**
     * raw native HttpRequest call.
     * @param request native HttpRequest
     * @return Void or a CompletableFuture of a native HttpResponse.
     */
    CompletableFuture<?> nativeCall(NR request);

    CompletableFuture<?> call(R request, AtomicInteger attempts) throws RetryException;

    void setRateLimiter(RateLimiter<E> rateLimiter);

    Optional<RateLimiter<E>> getRateLimiter();

    String getPermitsPerExecution();




    default Stopwatch rateLimitCall(R request) throws InterruptedException {
        Optional<RateLimiter<E>> limiter = getRateLimiter();
        Stopwatch rateLimitedStopWatch = null;
        if (limiter.isPresent()) {
            rateLimitedStopWatch = Stopwatch.createStarted();
            RateLimiter<E> httpExchangeRateLimiter = limiter.get();
            String permitsPerExecution = getPermitsPerExecution();
            if (RATE_LIMITER_REQUEST_LENGTH_PER_CALL.equals(permitsPerExecution)) {
                long length = request.getLength();
                httpExchangeRateLimiter.acquirePermits(Math.toIntExact(length));
                LOGGER.warn("{} permits acquired for request:'{}'", length, request);
            } else {
                httpExchangeRateLimiter.acquirePermits(ONE_REQUEST);
                LOGGER.warn("1 permit acquired for request:'{}'", request);
            }
        } else {
            LOGGER.trace("no rate limiter is configured");
        }
        return rateLimitedStopWatch;
    }

    default RateLimiter<E> buildRateLimiter(Map<String, String> configMap, String configurationId) {
        RateLimiter<E> myRateLimiter = null;
        if (configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)) {
            long maxExecutions = Long.parseLong(configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            LOGGER.trace("configuration '{}' : maxExecutions :{}", configurationId, maxExecutions);
            long periodInMs = Long.parseLong(Optional.ofNullable(configMap.get(RATE_LIMITER_PERIOD_IN_MS)).orElse(1000 + ""));
            LOGGER.trace("configuration '{}' : periodInMs :{}", configurationId, periodInMs);
            if (configMap.containsKey(RATE_LIMITER_SCOPE) && STATIC_SCOPE.equalsIgnoreCase(configMap.get(RATE_LIMITER_SCOPE))) {
                LOGGER.trace("configuration '{}' : rateLimiter scope is 'static'", configurationId);
                Optional<RateLimiter<E>> sharedRateLimiter = Optional.ofNullable(SHARED_RATE_LIMITERS.get(configurationId));
                if (sharedRateLimiter.isPresent()) {
                    myRateLimiter = sharedRateLimiter.get();
                    LOGGER.trace("configuration '{}' : rate limiter is already shared", configurationId);
                } else {
                    myRateLimiter = RateLimiter.<E>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
                    registerRateLimiter(configurationId, myRateLimiter);
                }
            } else {
                myRateLimiter = RateLimiter.<E>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
            }
            RateLimiterConfig<E> rateLimiterConfig = myRateLimiter.getConfig();
            LOGGER.trace("configuration '{}' rate limiter configured : 'maxRate:{},maxPermits{},period:{},maxWaitTime:{}'",
                    configurationId,
                    rateLimiterConfig.getMaxRate(),
                    rateLimiterConfig.getMaxPermits(),
                    rateLimiterConfig.getPeriod(),
                    rateLimiterConfig.getMaxWaitTime()
            );
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("configuration '{}' : rate limiter is not configured", configurationId);
                LOGGER.trace(Joiner.on(",\n").withKeyValueSeparator("=").join(configMap.entrySet()));
            }
        }
        return myRateLimiter;
    }

    private void registerRateLimiter(String configurationId, RateLimiter<E> rateLimiter) {
        Preconditions.checkNotNull(configurationId, "we cannot register a rateLimiter for a 'null' configurationId");
        Preconditions.checkNotNull(rateLimiter, "we cannot register a 'null' rate limiter for the configurationId " + configurationId);
        LOGGER.info("registration of a shared rateLimiter for the configurationId '{}'", configurationId);
        SHARED_RATE_LIMITERS.put(configurationId, rateLimiter);
    }


}
