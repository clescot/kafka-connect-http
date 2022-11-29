package com.github.clescot.kafka.connect.http.sink.client;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition;
import com.google.common.base.Stopwatch;
import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class  AbstractHttpClient<Req,Res> implements HttpClient<Req,Res>{
    private Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();
    RateLimiter<HttpExchange> defaultRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, Duration.of(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ChronoUnit.MILLIS)).build();

    public void setDefaultRateLimiter(long periodInMs, long maxExecutions) {
        this.defaultRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
    }

    @Override
    public  HttpExchange call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {
        Pattern successPattern = getSuccessPattern(httpRequest);
        Req request = buildRequest(httpRequest);
        LOGGER.info("request: {}", request.toString());
        try {
            this.defaultRateLimiter.acquirePermits(ONE_HTTP_REQUEST);
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire execution permit from the rate limiter {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {

            OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
            Res response = nativeCall(request);
            LOGGER.info("response: {}", response);
            stopwatch.stop();

            HttpResponse httpResponse = buildResponse(response, successPattern);
            LOGGER.info("duration: {}", stopwatch);
            return getHttpExchange(httpRequest, httpResponse, stopwatch, now, attempts);
        } catch (HttpException e) {
            LOGGER.error("Failed to call web service {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        } finally {
            if (stopwatch.isRunning()) {
                stopwatch.stop();
            }
        }


    }

    protected abstract HttpResponse buildResponse(Res response, Pattern successPattern);

    public void setDefaultRetryPolicy(Integer retries, Long retryDelayInMs, Long retryMaxDelayInMs, Double retryDelayFactor, Long retryJitterInMs) {
        this.defaultRetryPolicy = Optional.of(buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitterInMs));
    }

    protected int getSuccessfulStatusCodeOrThrowRetryException(int responseStatusCode, Pattern pattern) {
        Matcher matcher = pattern.matcher("" + responseStatusCode);

        if (!matcher.matches() && responseStatusCode >= 500) {
            throw new HttpException("response status code:" + responseStatusCode + " does not match status code success regex " + pattern.pattern());
        }
        return responseStatusCode;
    }



}
