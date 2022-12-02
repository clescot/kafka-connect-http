package com.github.clescot.kafka.connect.http.sink.client;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.google.common.base.Stopwatch;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class  AbstractHttpClient<Req,Res> implements HttpClient<Req,Res>{

    @Override
    public  HttpExchange call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {


        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            Req request = buildRequest(httpRequest);
            LOGGER.info("request: {}", request.toString());
            OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
            Res response = nativeCall(request);
            LOGGER.info("response: {}", response);
            stopwatch.stop();
            HttpResponse httpResponse = buildResponse(response);
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


    protected abstract HttpResponse buildResponse(Res response);
    protected abstract Res nativeCall(Req request);
}
