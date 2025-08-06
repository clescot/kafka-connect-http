package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.Client;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.RATE_LIMITER_REQUEST_LENGTH_PER_CALL;

/**
 * execute the HTTP call.
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 */
public interface HttpClient<R, S>  extends Client {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);

    String THROWABLE_CLASS = "throwable.class";
    String THROWABLE_MESSAGE = "throwable.message";


    static HttpExchange buildHttpExchange(HttpRequest httpRequest,
                                           HttpResponse httpResponse,
                                           Stopwatch stopwatch,
                                           OffsetDateTime now,
                                           AtomicInteger attempts,
                                           boolean success) {
        Preconditions.checkNotNull(httpRequest, "'httpRequest' is null");
        return HttpExchange.Builder.anHttpExchange()
                //request
                .withHttpRequest(httpRequest)
                //response
                .withHttpResponse(httpResponse)
                //technical metadata
                //time elapsed during http call
                .withDuration(stopwatch.elapsed(TimeUnit.MILLISECONDS))
                //at which moment occurs the beginning of the http call
                .at(now)
                .withAttempts(attempts)
                .withSuccess(success)
                .build();
    }


    /**
     * convert an {@link HttpRequest} into a native (from the implementation) request.
     *
     * @param httpRequest http request to build.
     * @return native request.
     */
    R buildNativeRequest(HttpRequest httpRequest);


    HttpRequest buildRequest(R nativeRequest);



    default CompletableFuture<HttpExchange> call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {

        Stopwatch rateLimitedStopWatch = Stopwatch.createStarted();
        CompletableFuture<S> response;
        LOGGER.debug("httpRequest: {}", httpRequest);
        R request = buildNativeRequest(httpRequest);
        LOGGER.debug("native request: {}", request);
        OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
        try {
            Optional<RateLimiter<HttpExchange>> limiter = getRateLimiter();
            if (limiter.isPresent()) {
                RateLimiter<HttpExchange> httpExchangeRateLimiter = limiter.get();
                String permitsPerExecution = getPermitsPerExecution();
                if(RATE_LIMITER_REQUEST_LENGTH_PER_CALL.equals(permitsPerExecution)){
                    long length = httpRequest.getLength();
                    httpExchangeRateLimiter.acquirePermits(Math.toIntExact(length));
                    LOGGER.warn("{} permits acquired for request:'{}'", length,request);
                }else{
                    httpExchangeRateLimiter.acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                    LOGGER.warn("1 permit acquired for request:'{}'", request);
                }
            }else{
                LOGGER.trace("no rate limiter is configured");
            }
            Stopwatch directStopWatch = Stopwatch.createStarted();
            response = nativeCall(request);

        Preconditions.checkNotNull(response, "response is null");

        return response.thenApply(this::buildResponse)
                .thenApply(myResponse -> {
                            directStopWatch.stop();
                            rateLimitedStopWatch.stop();
                            if(LOGGER.isTraceEnabled()) {
                                LOGGER.trace("httpResponse: {}", myResponse);
                            }
                    Integer responseStatusCode = myResponse.getStatusCode();
                    String responseStatusMessage = myResponse.getStatusMessage();
                    long directElaspedTime = directStopWatch.elapsed(TimeUnit.MILLISECONDS);
                    //elapsed time contains rate limiting waiting time + + local code execution time + network time + remote server-side execution time
                    long overallElapsedTime = rateLimitedStopWatch.elapsed(TimeUnit.MILLISECONDS);
                    long waitingTime = overallElapsedTime - directElaspedTime;
                    LOGGER.info("[{}] {} {} : {} '{}' (direct : '{}' ms, waiting time :'{}'ms overall : '{}' ms)",Thread.currentThread().getId(),httpRequest.getMethod(),httpRequest.getUrl(),responseStatusCode,responseStatusMessage, directElaspedTime,waitingTime,overallElapsedTime);
                    return buildHttpExchange(httpRequest, myResponse, directStopWatch, now, attempts, responseStatusCode < 400 ? SUCCESS : FAILURE);
                        }
                ).exceptionally((throwable-> {
                    HttpResponse httpResponse = new HttpResponse(400,throwable.getMessage());
                    Map<String, List<String>> responseHeaders = Maps.newHashMap();
                    responseHeaders.put(THROWABLE_CLASS, Lists.newArrayList(throwable.getCause().getClass().getName()));
                    responseHeaders.put(THROWABLE_MESSAGE, Lists.newArrayList(throwable.getCause().getMessage()));
                    httpResponse.setHeaders(responseHeaders);
                    LOGGER.error(throwable.toString());
                    return buildHttpExchange(httpRequest, httpResponse, rateLimitedStopWatch, now, attempts,FAILURE);
                }));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HttpException(e);
        }
    }



    /**
     * convert a native response (from the implementation) to an {@link HttpResponse}.
     *
     * @param response native response
     * @return HttpResponse
     */

    HttpResponse buildResponse(S response);

    /**
     * raw native HttpRequest call.
     * @param request native HttpRequest
     * @return CompletableFuture of a native HttpResponse.
     */
    CompletableFuture<S> nativeCall(R request);


    Integer getStatusMessageLimit();

    void setStatusMessageLimit(Integer statusMessageLimit);

    Integer getHeadersLimit();

    void setHeadersLimit(Integer headersLimit);

    Integer getBodyLimit();

    String getPermitsPerExecution();

    void setBodyLimit(Integer bodyLimit);

    void setRateLimiter(RateLimiter<HttpExchange> rateLimiter);

    Optional<RateLimiter<HttpExchange>> getRateLimiter();

    TrustManagerFactory getTrustManagerFactory();


    void setTrustManagerFactory(TrustManagerFactory trustManagerFactory);
}
