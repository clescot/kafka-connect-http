package com.github.clescot.kafka.connect.http.sink.service;

import com.github.clescot.kafka.connect.http.sink.config.AckConfig;
import com.github.clescot.kafka.connect.http.sink.model.Acknowledgement;
import com.google.common.base.Preconditions;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class AckSender {

    public static final String HTTP_HEADER_KEY_VALUE_SEPARATOR = ":";
    private static AckSender instance;
    private final static Logger LOGGER = LoggerFactory.getLogger(AckSender.class);
    public static synchronized AckSender getInstance(AckConfig config, KafkaFailSafeProducer<String, byte[]> producer,  Converter valueConverter) {
        if (instance == null) {
            instance = new AckSender(config, producer, valueConverter);
        }
        return instance;
    }

    protected static void clear(){
        instance = null;
    }

    public static AckSender getCurrentInstance() {
        return instance;
    }

    public static void clearCurrentInstance(){
        instance = null;
    }

    private KafkaFailSafeProducer<String, byte[]> producer;
    private Converter valueConverter;
    private String ackTopic;
    private Schema ackSchema;


    private AckSender(AckConfig config, KafkaFailSafeProducer<String, byte[]> producer,  Converter valueConverter) {

        this.producer = producer;
        this.valueConverter = valueConverter;
        LOGGER.info("multiplicator kafka producer is created");
        this.ackTopic = config.getAckTopic();
        this.ackSchema = config.getAckSchema();
    }

    public RecordMetadata send(Acknowledgement ack) {
        Preconditions.checkNotNull(ack,"ack must not be null");
        byte[] bytesValue = convertValue(ack);
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(this.ackTopic, ack.getCorrelationId(), bytesValue);
        RecordMetadata send;
        try {
            send = this.producer.send(producerRecord).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Successful message sending to topic : {} -> acknownledgment : {}, ", this.ackTopic,ack.toString());
        return send;
    }

    protected byte[] convertValue(Acknowledgement ack) {
        Preconditions.checkNotNull(ack,"acknowledgement is null");
        Struct value = new Struct(this.ackSchema);

        Preconditions.checkNotNull(ack.getCorrelationId(),"acknowledgement 'correlationId' is null");
        value.put("correlationId", ack.getCorrelationId());

        Preconditions.checkNotNull(ack.getRequestId(),"acknowledgement 'requestId' is null");
        value.put("requestId", ack.getRequestId());

        Preconditions.checkState(ack.getStatusCode()!=null && ack.getStatusCode()>0,"acknowledgement 'statusCode' is null");
        value.put("statusCode", ack.getStatusCode());

        Preconditions.checkNotNull(ack.getStatusMessage(),"acknowledgement 'statusMessage' is null");
        value.put("statusMessage", ack.getStatusMessage());

        Preconditions.checkNotNull(ack.getResponseHeaders(),"acknowledgement 'responseHeaders' is null");
        List<String> responseHeaders = Lists.newArrayList();
        for (Map.Entry<String,String> responseHeader : ack.getResponseHeaders()) {
            responseHeaders.add(responseHeader.getKey()+ HTTP_HEADER_KEY_VALUE_SEPARATOR +responseHeader.getValue());
        }
        value.put("responseHeaders",responseHeaders);

        Preconditions.checkNotNull(ack.getResponseBody(),"acknowledgement 'responseBody' is null");
        value.put("responseBody", ack.getResponseBody());

        Preconditions.checkNotNull(ack.getMethod(),"acknowledgement 'method' is null");
        value.put("method", ack.getMethod());

        Preconditions.checkNotNull(ack.getRequestUri(),"acknowledgement 'requestUri' is null");
        value.put("requestUri", ack.getRequestUri());

        Preconditions.checkNotNull(ack.getRequestHeaders(),"acknowledgement 'requestHeaders' is null");
        List<String> requestHeaders = Lists.newArrayList();
        for (Map.Entry<String,String> requestHeader : ack.getRequestHeaders()) {
            requestHeaders.add(requestHeader.getKey()+ HTTP_HEADER_KEY_VALUE_SEPARATOR +requestHeader.getValue());
        }
        value.put("requestHeaders", requestHeaders);

        Preconditions.checkNotNull(ack.getRequestBody(),"acknowledgement 'requestBody' is null");
        value.put("requestBody", ack.getRequestBody());

        Preconditions.checkArgument(ack.getDurationInMillis()>-1,"acknowledgement 'durationInMillis' is negative");
        value.put("durationInMillis", ack.getDurationInMillis());

        Preconditions.checkNotNull(ack.getMoment(),"acknowledgement 'moment' is null");
        //we format the moment in 8601 iso format, zoned to UTC
        value.put("moment", ack.getMoment().format(DateTimeFormatter.ISO_INSTANT));

        Preconditions.checkNotNull(ack.getAttempts(),"acknowledgement 'attempts' is null");
        value.put("attempts", ack.getAttempts().intValue());
        value.validate();
        return this.valueConverter.fromConnectData(this.ackTopic, this.ackSchema, value);
    }

    public void close() {
        LOGGER.info("Closing multiplicator kafka producer");
        producer.flush();
        producer.close();
    }

    public void setValueConverter(Converter valueConverter) {
        this.valueConverter = valueConverter;
    }

    //for mock
    public void setProducer(KafkaProducer<String, byte[]> producer) {
        this.producer.setKafkaProducer(producer);
    }

    //for mock
    public void setSupplier(Supplier<KafkaProducer<String,byte[]>> supplier){
        this.producer.setSupplier(supplier);
    }
}
