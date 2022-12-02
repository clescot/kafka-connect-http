package com.github.clescot.kafka.connect.http.sink.client;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public interface HttpClient<Req, Res> {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    String BLANK_RESPONSE_CONTENT = "";
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    final static Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);



    /**
     * @param httpRequest
     * @return
     */




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



    <Res> Res buildRequest(HttpRequest httpRequest);

    HttpExchange call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException;
}
