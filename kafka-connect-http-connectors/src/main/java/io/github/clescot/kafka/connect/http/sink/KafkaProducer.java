package io.github.clescot.kafka.connect.http.sink;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class KafkaProducer<K,V> implements Producer<K,V> {


    private Producer<K, V> producer;
    private final boolean mock;

    public KafkaProducer(boolean mock) {
        this.mock = mock;
    }
    public KafkaProducer() {
        this.mock = false;
    }
    public void configure(Map<String, Object> producerSettings,Serializer<K> keySerializer,Serializer<V> valueSerializer){
        if(mock){
            producer = new MockProducer<>();
        }else{
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerSettings,keySerializer,valueSerializer);
        }
    }

    @Override
    public void initTransactions() {
        producer.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        producer.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
        producer.sendOffsetsToTransaction(offsets,consumerGroupId);
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        producer.sendOffsetsToTransaction(offsets,groupMetadata);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        producer.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        producer.abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> myRecord) {
        return producer.send(myRecord);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> myRecord, Callback callback) {
        return producer.send(myRecord,callback);
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void close(Duration timeout) {
        producer.close(timeout);
    }
}
