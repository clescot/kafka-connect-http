package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.RequestResponseClient;
import io.github.clescot.kafka.connect.http.client.config.AddSuccessStatusToHttpExchangeFunction;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.net.CookiePolicy;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.RATE_LIMITER_REQUEST_LENGTH_PER_CALL;

/**
 * execute the HTTP call.
 *
 * @param <NR> native HttpRequest
 * @param <NS> native HttpResponse
 */
@SuppressWarnings({"java:S119"})
public interface HttpClient<NR, NS> extends RequestResponseClient<HttpRequest, NR, HttpResponse, NS, HttpExchange>, Cloneable {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);

    String THROWABLE_CLASS = "throwable.class";
    String THROWABLE_MESSAGE = "throwable.message";

    default HttpExchange buildExchange(HttpRequest request,
                                       HttpResponse response,
                                       Stopwatch stopwatch,
                                       OffsetDateTime now,
                                       AtomicInteger attempts,
                                       boolean success,
                                       Map<String, Object> attributes,
                                       Map<String, Long> timings) {
        Preconditions.checkNotNull(request, "'httpRequest' is null");
        return HttpExchange.Builder.anHttpExchange()
                //request
                .withHttpRequest(request)
                //response
                .withHttpResponse(response)
                //technical metadata
                //time elapsed during http call
                .withDuration(stopwatch!=null?stopwatch.elapsed(TimeUnit.MILLISECONDS):0)
                //at which moment occurs the beginning of the http call
                .at(now)
                .withAttempts(attempts)
                .withSuccess(success)
                .withAttributes(attributes != null && !attributes.isEmpty() ? attributes : Maps.newHashMap())
                .withTimings(timings)
                .build();
    }

    default CompletableFuture<HttpExchange> call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {

        Stopwatch rateLimitedStopWatch = null;
        CompletableFuture<NS> response;
        LOGGER.debug("httpRequest: {}", httpRequest);
        NR request = buildNativeRequest(httpRequest);
        LOGGER.debug("native request: {}", request);
        OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
        try {
            Optional<RateLimiter<HttpExchange>> limiter = getRateLimiter();
            if (limiter.isPresent()) {
                RateLimiter<HttpExchange> httpExchangeRateLimiter = limiter.get();
                rateLimitedStopWatch = Stopwatch.createStarted();
                String permitsPerExecution = getPermitsPerExecution();
                if (RATE_LIMITER_REQUEST_LENGTH_PER_CALL.equals(permitsPerExecution)) {
                    long length = httpRequest.getLength();
                    httpExchangeRateLimiter.acquirePermits(Math.toIntExact(length));
                    LOGGER.warn("{} permits acquired for request:'{}'", length, request);
                } else {
                    httpExchangeRateLimiter.acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                    LOGGER.warn("1 permit acquired for request:'{}'", request);
                }
            } else {
                LOGGER.trace("no rate limiter is configured");
            }
            Stopwatch directStopWatch = Stopwatch.createStarted();
            response = nativeCall(request);

            Preconditions.checkNotNull(response, "response is null");

            Stopwatch finalRateLimitedStopWatch = rateLimitedStopWatch;
            return response.thenApply(this::buildResponse)
                    .thenApply(myResponse -> {
                                directStopWatch.stop();
                                if (finalRateLimitedStopWatch != null) {
                                    finalRateLimitedStopWatch.stop();
                                }
                                if (LOGGER.isTraceEnabled()) {
                                    LOGGER.trace("httpResponse: {}", myResponse);
                                }
                                Map<String, Long> timings = getTimings(request, response);
                                LOGGER.debug("timings : {}", timings);
                                Integer responseStatusCode = myResponse.getStatusCode();
                                String responseStatusMessage = myResponse.getStatusMessage();
                                long directElapsedTime = directStopWatch.elapsed(TimeUnit.MILLISECONDS);
                                timings.put("directElapsedTime", directElapsedTime);
                                //elapsed time contains rate limiting waiting time + local code execution time + network time + remote server-side execution time
                                //if ratelimiting is not set, overallElapsedTime == directElaspedTime
                                long overallElapsedTime = finalRateLimitedStopWatch!=null?finalRateLimitedStopWatch.elapsed(TimeUnit.MILLISECONDS):directElapsedTime;
                                timings.put("overallElapsedTime", overallElapsedTime);
                                long rateLimitingWaitingTime = overallElapsedTime - directElapsedTime;
                                timings.put("rateLimitingWaitingTime", rateLimitingWaitingTime);
                                LOGGER.info("[{}] {} {} : {} '{}' (direct : '{}' ms, rate limiting waiting time :'{}'ms overall : '{}' ms)",
                                        Thread.currentThread().getId(),
                                        httpRequest.getMethod(),
                                        httpRequest.getUrl(),
                                        responseStatusCode,
                                        responseStatusMessage,
                                        directElapsedTime,
                                        rateLimitingWaitingTime,
                                        overallElapsedTime
                                );
                                return buildExchange(httpRequest, myResponse, directStopWatch, now, attempts, responseStatusCode < 400 ? SUCCESS : FAILURE,
                                        Maps.newHashMap(),
                                        timings);
                            }
                    ).exceptionally((throwable -> {
                        HttpResponse httpResponse = new HttpResponse(400, throwable.getMessage());
                        Map<String, List<String>> responseHeaders = Maps.newHashMap();
                        responseHeaders.put(THROWABLE_CLASS, Lists.newArrayList(throwable.getCause().getClass().getName()));
                        responseHeaders.put(THROWABLE_MESSAGE, Lists.newArrayList(throwable.getCause().getMessage()));
                        httpResponse.setHeaders(responseHeaders);
                        LOGGER.error(throwable.toString());
                        return buildExchange(httpRequest, httpResponse, finalRateLimitedStopWatch, now, attempts, FAILURE,
                                Maps.newHashMap(),
                                Maps.newHashMap());
                    }));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HttpException(e);
        }
    }

    HttpClient<NR, NS> customizeForUser(String vuId);

    CookiePolicy getCookiePolicy();

    Function<HttpRequest, HttpRequest> getEnrichRequestFunction();

    Integer getStatusMessageLimit();

    void setStatusMessageLimit(Integer statusMessageLimit);

    Integer getHeadersLimit();

    void setHeadersLimit(Integer headersLimit);

    Integer getBodyLimit();

    void setBodyLimit(Integer bodyLimit);

    TrustManagerFactory getTrustManagerFactory();

    void setTrustManagerFactory(TrustManagerFactory trustManagerFactory);

    void setAddSuccessStatusToHttpExchangeFunction(Pattern addSuccessStatusToHttpExchangeFunction);

    AddSuccessStatusToHttpExchangeFunction getAddSuccessStatusToHttpExchangeFunction();
}
