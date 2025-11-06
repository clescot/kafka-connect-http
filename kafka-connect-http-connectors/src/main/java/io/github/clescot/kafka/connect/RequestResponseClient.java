package io.github.clescot.kafka.connect;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.client.RetryException;
import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A client that can handle both request and response.
 * @param <R> request type
 * @param <NR> native request type
 * @param <S> response type
 * @param <NS> native response type
 * @param <E> exchange type
 */
@SuppressWarnings("java:S119")
public interface RequestResponseClient<R extends Request, NR, S extends Response, NS, E extends Exchange<R,S>> extends RequestClient<R, NR,E>, ResponseClient<S, NS,E> {
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    boolean FAILURE = false;
    String THROWABLE_CLASS = "throwable.class";
    String THROWABLE_MESSAGE = "throwable.message";


    E buildExchange(R request,
                    S response,
                    Stopwatch stopwatch,
                    OffsetDateTime now,
                    AtomicInteger attempts,
                    Map<String,Object> attributes,
                    Map<String,Long> timings);

    /**
     * raw native Request call.
     * @param request native Request
     * @return CompletableFuture of a native Response.
     */
    CompletableFuture<NS> nativeCall(NR request);

    Map<String, Long> getTimings(NR request, CompletableFuture<NS> response);


    default CompletableFuture<E> call(R request, AtomicInteger attempts) throws RetryException, HttpException {

        LOGGER.debug("request: {}", request);
        NR nativeRequest = buildNativeRequest(request);
        LOGGER.debug("native request: {}", nativeRequest);
        OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
        CompletableFuture<NS> nativeResponse;
        try {

            Stopwatch rateLimitedStopWatch = rateLimitCall(request);
            Stopwatch directStopWatch = Stopwatch.createStarted();

            //real call is executed here
            nativeResponse = nativeCall(nativeRequest);

            Preconditions.checkNotNull(nativeResponse, "response is null");

            return nativeResponse.thenApply(this::buildResponse)
                    .thenApply(myResponse -> {
                                directStopWatch.stop();
                                if (rateLimitedStopWatch != null) {
                                    rateLimitedStopWatch.stop();
                                }
                                if (LOGGER.isTraceEnabled()) {
                                    LOGGER.trace("httpResponse: {}", myResponse);
                                }
                                Map<String, Long> timings = getTimings(nativeRequest, nativeResponse);
                                LOGGER.debug("timings : {}", timings);
                                long directElapsedTime = directStopWatch.elapsed(TimeUnit.MILLISECONDS);
                                timings.put("directElapsedTime", directElapsedTime);
                                //elapsed time contains rate limiting waiting time + local code execution time + network time + remote server-side execution time
                                //if ratelimiting is not set, overallElapsedTime == directElaspedTime
                                long overallElapsedTime = rateLimitedStopWatch!=null?rateLimitedStopWatch.elapsed(TimeUnit.MILLISECONDS):directElapsedTime;
                                timings.put("overallElapsedTime", overallElapsedTime);
                                long rateLimitingWaitingTime = overallElapsedTime - directElapsedTime;
                                timings.put("rateLimitingWaitingTime", rateLimitingWaitingTime);
                                LOGGER.info("[{}]  {} :  (direct : '{}' ms, rate limiting waiting time :'{}'ms overall : '{}' ms)",
                                        Thread.currentThread().getId(),
                                        request,
                                        directElapsedTime,
                                        rateLimitingWaitingTime,
                                        overallElapsedTime
                                );
                                return buildExchange(request, myResponse, directStopWatch, now, attempts,
                                        Maps.newHashMap(),
                                        timings);
                            }
                    ).exceptionally((throwable -> {
                        return getErrorResponse(request, attempts, throwable, rateLimitedStopWatch, now);
                    }));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    E getErrorResponse(R request, AtomicInteger attempts, Throwable throwable, Stopwatch rateLimitedStopWatch, OffsetDateTime now);

}
