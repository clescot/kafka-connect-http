package io.github.clescot.kafka.connect.http.sink.publish;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class KafkaProducer<K, V> implements Producer<K, V> {


    private Producer<K, V> producer;


    public KafkaProducer(Producer<K, V> producer) {
        this.producer = producer;
    }

    public KafkaProducer() {
    }

    public void configure(Map<String, Object> producerSettings, Serializer<K> keySerializer, Serializer<V> valueSerializer) {

            if (!MockProducer.class.isAssignableFrom(producer.getClass())) {
                producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerSettings, keySerializer, valueSerializer);
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
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
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
    public void registerMetricForSubscription(KafkaMetric kafkaMetric) {
        producer.registerMetricForSubscription(kafkaMetric);
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric kafkaMetric) {
        producer.unregisterMetricFromSubscription(kafkaMetric);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> myRecord) {
        return producer.send(myRecord);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> myRecord, Callback callback) {
        return producer.send(myRecord, callback);
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
    public Uuid clientInstanceId(Duration duration) {
        return producer.clientInstanceId(duration);
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
