package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.client.HttpClient.*;

public class QueueProducer implements Runnable {
    private Queue<Acknowledgement> transferQueue;


    private long numberOfSuccessfulMessages;
    private long numberOfErrorMessages;

    public AtomicInteger numberOfProducedMessages = new AtomicInteger();


    public QueueProducer(Queue<Acknowledgement> transferQueue, long numberOfSuccessfulMessages, long numberOfErrorMessages) {
        this.transferQueue = transferQueue;
        this.numberOfSuccessfulMessages = numberOfSuccessfulMessages;
        this.numberOfErrorMessages = numberOfErrorMessages;
    }

    @Override
    public void run() {
        for (int i = 0; i < numberOfSuccessfulMessages; i++) {
            transferQueue.offer(getAcknowledgement(SUCCESS));
            numberOfProducedMessages.incrementAndGet();
        }
        for (int i = 0; i < numberOfErrorMessages; i++) {
            transferQueue.offer(getAcknowledgement(FAILURE));
            numberOfProducedMessages.incrementAndGet();
        }
    }

    private static Acknowledgement getAcknowledgement(boolean success) {
        Map<String,String> requestheaders = Maps.newHashMap();
        requestheaders.put("X-Request-ID","sdqd-qsdqd-446564");
        requestheaders.put("X-Correlation-ID","222-qsdqd-446564");
        requestheaders.put("Content-Type","application/json");
        return success?getSuccessfulAcknowledgement(requestheaders):getErrorAcknowledgement(requestheaders);
    }

    @NotNull
    private static Acknowledgement getSuccessfulAcknowledgement(Map<String, String> requestheaders) {
        Map<String,String> responseHeaders = Maps.newHashMap();
        responseHeaders.put("Content-Type","application/json");
        return Acknowledgement.AcknowledgementBuilder.anAcknowledgement()
                //tracing headers
                .withRequestId(UUID.randomUUID().toString())
                .withCorrelationId("my-correlation-id")
                //request
                .withRequestUri("http://fakeUri.com")
                .withRequestHeaders(requestheaders)
                .withMethod("GET")
                .withRequestBody("requestBody")
                //response
                .withResponseHeaders(responseHeaders)
                .withResponseBody("body")
                .withStatusCode(200)
                .withStatusMessage("OK")
                //technical metadata
                //time elapsed during http call
                .withDuration(469878798L)
                //at which moment occurs the beginning of the http call
                .at(OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID)))
                .withAttempts(new AtomicInteger(1))
                .withSuccess(SUCCESS)
                .build();
    }

    @NotNull
    private static Acknowledgement getErrorAcknowledgement(Map<String, String> requestheaders) {
        Map<String,String> responseHeaders = Maps.newHashMap();
        responseHeaders.put("Content-Type","application/json");
        return Acknowledgement.AcknowledgementBuilder.anAcknowledgement()
                //tracing headers
                .withRequestId(UUID.randomUUID().toString())
                .withCorrelationId("another-correlation-id")
                //request
                .withRequestUri("http://fakeUri.com")
                .withRequestHeaders(requestheaders)
                .withMethod("GET")
                .withRequestBody("requestBody")
                //response
                .withResponseHeaders(responseHeaders)
                .withResponseBody("Internal server error ... please retry later")
                .withStatusCode(500)
                .withStatusMessage("Internal Server Error")
                //technical metadata
                //time elapsed during http call
                .withDuration(465558798L)
                //at which moment occurs the beginning of the http call
                .at(OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID)))
                .withAttempts(new AtomicInteger(1))
                .withSuccess(FAILURE)
                .build();
    }
}
