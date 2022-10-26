package com.github.clescot.kafka.connect.http.sink.service;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.Future;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaFailSafeProducerTest {


    @Test
    public void test_nominal_case(){
        KafkaProducer kafkaProducer = mock(KafkaProducer.class);
        Future<RecordMetadata> recordMetadataFuture = mock(Future.class);
        when(kafkaProducer.send(any()))
                .thenAnswer(new Answer<Object>() {
                    private int count = 0;
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        if (count<1) {
                            count++;
                            throw new IllegalStateException("I have failed.");
                        }

                        return recordMetadataFuture;
                    }
                });
        KafkaFailSafeProducer producer = new KafkaFailSafeProducer(()-> kafkaProducer,2);
        producer.send(new ProducerRecord("topic","value"));

    }
}