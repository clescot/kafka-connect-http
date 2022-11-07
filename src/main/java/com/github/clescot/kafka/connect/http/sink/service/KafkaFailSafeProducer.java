package com.github.clescot.kafka.connect.http.sink.service;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class KafkaFailSafeProducer<K, V> implements Producer<K, V> {

    private KafkaProducer<K,V> kafkaProducer;
    private Supplier<KafkaProducer<K, V>> supplier;
    private RetryPolicy<Object> retryPolicy;
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaFailSafeProducer.class);

    public KafkaFailSafeProducer(Supplier<KafkaProducer<K, V>> supplier,int maxRetries) {
        this.supplier = supplier;
        kafkaProducer = supplier.get();
         retryPolicy = RetryPolicy.builder().handle(Exception.class).withMaxRetries(maxRetries).build();
    }


    @Override
    public void initTransactions() {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.initTransactions());
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.beginTransaction());
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.sendOffsetsToTransaction(offsets,consumerGroupId));
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, ConsumerGroupMetadata consumerGroupMetadata) throws ProducerFencedException {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.sendOffsetsToTransaction(map,consumerGroupMetadata));
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.commitTransaction());
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.abortTransaction());
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .get(()->kafkaProducer.send(record));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        return Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .get(()->kafkaProducer.send(record,callback));
    }

    @Override
    public void flush() {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.flush());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .get(()->kafkaProducer.partitionsFor(topic));
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .get(()->kafkaProducer.metrics());
    }

    @Override
    public void close() {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()-> kafkaProducer.close());

    }

    @Override
    public void close(Duration timeout) {
        Failsafe.with(retryPolicy)
                .onFailure(throwable -> buildNewProducer())
                .run(()->kafkaProducer.close(timeout));
    }

    public void buildNewProducer(){
        LOGGER.info("rebuild kafka producer");
        kafkaProducer = supplier.get();
    }

    public void setKafkaProducer(KafkaProducer<K, V> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void setSupplier(Supplier<KafkaProducer<K, V>> supplier) {
        this.supplier = supplier;
    }
}
