package com.github.clescot.kafka.connect.http.sink.client;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.google.common.base.Stopwatch;
import dev.failsafe.RetryPolicy;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class  AbstractHttpClient<Req,Res> implements HttpClient<Req,Res>{
    private Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();

    @Override
    public  HttpExchange call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {


        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Pattern successPattern = getSuccessPattern(httpRequest);
            Req request = buildRequest(httpRequest);
            LOGGER.info("request: {}", request.toString());
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

    protected int getSuccessfulStatusCodeOrThrowRetryException(int responseStatusCode, Pattern pattern) throws HttpException{
        Matcher matcher = pattern.matcher("" + responseStatusCode);

        if (!matcher.matches() && responseStatusCode >= 500) {
            throw new HttpException("response status code:" + responseStatusCode + " does not match status code success regex " + pattern.pattern());
        }
        return responseStatusCode;
    }

    protected abstract HttpResponse buildResponse(Res response, Pattern successPattern);
    protected abstract Res nativeCall(Req request);
}
