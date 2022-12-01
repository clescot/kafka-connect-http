package com.github.clescot.kafka.connect.http.sink.client;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public interface HttpClient<Req, Res> {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();
    String BLANK_RESPONSE_CONTENT = "";
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
    Map<String, Pattern> httpSuccessCodesPatterns = Maps.newHashMap();


    /**
     * @param httpRequest
     * @return
     */
    default HttpExchange call(HttpRequest httpRequest) {
        HttpExchange httpExchange = null;

        if (httpRequest != null) {
            Optional<RetryPolicy<HttpExchange>> retryPolicyForCall = defaultRetryPolicy;
            AtomicInteger attempts = new AtomicInteger();
            try {
                attempts.addAndGet(1);
                if (retryPolicyForCall.isPresent()) {
                    httpExchange = Failsafe.with(List.of(retryPolicyForCall.get()))
                            .get(() -> call(httpRequest, attempts));
                } else {
                    return call(httpRequest, attempts);
                }
            } catch (Throwable throwable) {
                LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, throwable,
                        throwable.getMessage());
                return buildHttpExchange(
                        httpRequest,
                        new HttpResponse(SERVER_ERROR_STATUS_CODE, String.valueOf(throwable.getMessage()), BLANK_RESPONSE_CONTENT),
                        Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID)),
                        attempts,
                        FAILURE);
            }
        }
        return httpExchange;
    }

    default Pattern getSuccessPattern(HttpRequest httpRequest) {
        //by default, we don't resend any http call with a response between 100 and 499
        // 1xx is for protocol information (100 continue for example),
        // 2xx is for success,
        // 3xx is for redirection
        //4xx is for a client error
        //5xx is for a server error
        //only 5xx by default, trigger a resend

        /*
         *  HTTP Server status code returned
         *  3 cases can arise:
         *  * a success occurs : the status code returned from the ws server is matching the regexp => no retries
         *  * a functional error occurs: the status code returned from the ws server is not matching the regexp, but is lower than 500 => no retries
         *  * a technical error occurs from the WS server : the status code returned from the ws server does not match the regexp AND is equals or higher than 500 : retries are done
         */
        String wsSuccessCode = "^[1-4][0-9][0-9]$";

        if (this.httpSuccessCodesPatterns.get(wsSuccessCode) == null) {
            //Pattern.compile should be reused for performance, but wsSuccessCode can change....
            Pattern httpSuccessPattern = Pattern.compile(wsSuccessCode);
            httpSuccessCodesPatterns.put(wsSuccessCode, httpSuccessPattern);
        }
        Pattern pattern = httpSuccessCodesPatterns.get(wsSuccessCode);
        return pattern;
    }


    default HttpExchange getHttpExchange(HttpRequest httpRequest,
                                         HttpResponse httpResponse,
                                         Stopwatch stopwatch,
                                         OffsetDateTime now,
                                         AtomicInteger attempts) {
        return buildHttpExchange(
                httpRequest,
                httpResponse,
                stopwatch,
                now,
                attempts,
                SUCCESS
        );
    }


    default HttpExchange buildHttpExchange(HttpRequest httpRequest,
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

    default RetryPolicy<HttpExchange> buildRetryPolicy(Integer retries,
                                                       Long retryDelayInMs,
                                                       Long retryMaxDelayInMs,
                                                       Double retryDelayFactor,
                                                       Long retryJitterInMs) {
        return RetryPolicy.<HttpExchange>builder()
                //we retry only if the error comes from the WS server (server-side technical error)
                .handle(HttpException.class)
                .withBackoff(Duration.ofMillis(retryDelayInMs), Duration.ofMillis(retryMaxDelayInMs), retryDelayFactor)
                .withJitter(Duration.ofMillis(retryJitterInMs))
                .withMaxRetries(retries)
                .onRetry(listener -> LOGGER.warn("Retry ws call result:{}, failure:{}", listener.getLastResult(), listener.getLastException()))
                .onFailure(listener -> LOGGER.warn("ws call failed ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .onAbort(listener -> LOGGER.warn("ws call aborted ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .build();
    }

    <Res> Res buildRequest(HttpRequest httpRequest);

    HttpExchange call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException;

    Res nativeCall(Req request);

    void setDefaultRetryPolicy(Integer retries, Long retryDelayInMs, Long retryMaxDelayInMs, Double retryDelayFactor, Long retryJitterInMs);

}
