package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Lists;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.service.WsCaller.UTC_ZONE_ID;

public class QueueProducer implements Runnable {
    private TransferQueue<Acknowledgement> transferQueue;


    private Integer numberOfMessagesToProduce;

    public AtomicInteger numberOfProducedMessages = new AtomicInteger();


    public QueueProducer(TransferQueue<Acknowledgement> transferQueue,int numberOfMessagesToProduce) {
        this.transferQueue = transferQueue;
        this.numberOfMessagesToProduce = numberOfMessagesToProduce;
    }

    @Override
    public void run() {
        for (int i = 0; i < numberOfMessagesToProduce; i++) {
            try {
                transferQueue.transfer(getAcknowledgement());
                numberOfProducedMessages.incrementAndGet();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static Acknowledgement getAcknowledgement() {
        return Acknowledgement.AcknowledgementBuilder.anAcknowledgement()
                //tracing headers
                .withRequestId(UUID.randomUUID().toString())
                .withCorrelationId("my-correlation-id")
                //request
                .withRequestUri("http://fakeUri.com")
                .withRequestHeaders(Lists.newArrayList())
                .withMethod("GET")
                .withRequestBody("requestBody")
                //response
                .withResponseHeaders(Lists.newArrayList())
                .withResponseBody("body")
                .withStatusCode(200)
                .withStatusMessage("OK")
                //technical metadata
                //time elapsed during http call
                .withDuration(469878798L)
                //at which moment occurs the beginning of the http call
                .at(OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID)))
                .withAttempts(new AtomicInteger(1))
                .build();
    }
}
