package io.github.clescot.kafka.connect.http.sink.publish;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.sink.HttpSinkConnectorConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PublishConfigurerTest {

    @Nested
    class Build{
        @Test
        void test_nominal_case(){
            Assertions.assertDoesNotThrow(PublishConfigurer::build);
        }
    }

    @Nested
    class ConfigureProducerPublishMode{

        @Test
        void test_null_arg(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            Assertions.assertThrows(NullPointerException.class,()->publishConfigurer.configureProducerPublishMode(null, new KafkaProducer<>()));
        }

        @Test
        void test_empty_arg(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(Maps.newHashMap());
            Assertions.assertThrows(IllegalArgumentException.class,()->publishConfigurer.configureProducerPublishMode(httpSinkConnectorConfig, new KafkaProducer<>()));
        }
        @Test
        void test_only_bootstrap_servers(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            Node node = new Node(1,"localhost",9092);
            PartitionInfo partitionInfo = new PartitionInfo("success",0,node,new Node[]{},new Node[]{});
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList(partitionInfo));
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertDoesNotThrow(()->publishConfigurer.configureProducerPublishMode(httpSinkConnectorConfig, kafkaProducer));
        }

        @Test
        void test_bootstrap_servers_and_producer_format(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            originals.put("producer.format","json");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            Node node = new Node(1,"localhost",9092);
            PartitionInfo partitionInfo = new PartitionInfo("success",0,node,new Node[]{},new Node[]{});
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList(partitionInfo));
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertDoesNotThrow(()->publishConfigurer.configureProducerPublishMode(httpSinkConnectorConfig, kafkaProducer));
        }

        @Test
        void test_only_bootstrap_servers_with_empty_partition_info(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            Node node = new Node(1,"localhost",9092);
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList());
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertThrows(IllegalStateException.class,()->publishConfigurer.configureProducerPublishMode(httpSinkConnectorConfig, kafkaProducer));
        }
        @Test
        void test_only_bootstrap_servers_with_connectivity_error(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            when(cluster.partitionsForTopic(anyString())).thenThrow(new KafkaException());
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertThrows(KafkaException.class,()->publishConfigurer.configureProducerPublishMode(httpSinkConnectorConfig, kafkaProducer));
        }
    }

}