package io.github.clescot.kafka.connect;

import com.google.common.base.Stopwatch;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.client.RetryException;
import io.github.clescot.kafka.connect.http.core.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.RATE_LIMITER_REQUEST_LENGTH_PER_CALL;

/**
 *  A client interface that handles requests and native requests.
 * @param <R> request
 * @param <NR> native request
 */
@SuppressWarnings("java:S119")
public interface RequestClient<R extends Request,NR,E> extends Client<E>{


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
}
