package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Lists;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.service.WsCaller.UTC_ZONE_ID;

public class QueueProducer implements Runnable {
    private Queue<Acknowledgement> transferQueue;


    private Integer numberOfMessagesToProduce;

    public AtomicInteger numberOfProducedMessages = new AtomicInteger();


    public QueueProducer(Queue<Acknowledgement> transferQueue, int numberOfMessagesToProduce) {
        this.transferQueue = transferQueue;
        this.numberOfMessagesToProduce = numberOfMessagesToProduce;
    }

    @Override
    public void run() {
        for (int i = 0; i < numberOfMessagesToProduce; i++) {
            transferQueue.offer(getAcknowledgement());
            numberOfProducedMessages.incrementAndGet();
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
