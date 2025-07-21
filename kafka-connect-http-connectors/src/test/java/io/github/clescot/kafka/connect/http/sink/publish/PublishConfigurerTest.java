package io.github.clescot.kafka.connect.http.sink.publish;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static io.github.clescot.kafka.connect.http.core.queue.QueueFactory.DEFAULT_QUEUE_NAME;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
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
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>();
            Assertions.assertThrows(NullPointerException.class,()->publishConfigurer.configureProducerPublishMode(null, kafkaProducer));
        }

        @Test
        void test_empty_arg(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(Maps.newHashMap());
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>();
            Assertions.assertThrows(IllegalArgumentException.class,()->publishConfigurer.configureProducerPublishMode(httpConnectorConfig, kafkaProducer));
        }
        @Test
        void test_only_bootstrap_servers(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            Node node = new Node(1,"localhost",9092);
            PartitionInfo partitionInfo = new PartitionInfo("success",0,node,new Node[]{},new Node[]{});
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList(partitionInfo));
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new RoundRobinPartitioner(),new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertDoesNotThrow(()->publishConfigurer.configureProducerPublishMode(httpConnectorConfig, kafkaProducer));
        }

        @Test
        void test_bootstrap_servers_and_producer_format(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            originals.put("producer.format","json");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            Node node = new Node(1,"localhost",9092);
            PartitionInfo partitionInfo = new PartitionInfo("success",0,node,new Node[]{},new Node[]{});
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList(partitionInfo));
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new RoundRobinPartitioner(),new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertDoesNotThrow(()->publishConfigurer.configureProducerPublishMode(httpConnectorConfig, kafkaProducer));
        }

        @Test
        void test_bootstrap_servers_producer_format_and_schema_registry_options(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            originals.put("producer.format","json");
            originals.put(PRODUCER_MISSING_ID_CACHE_TTL_SEC,"30");
            originals.put(PRODUCER_MISSING_VERSION_CACHE_TTL_SEC,"30");
            originals.put(PRODUCER_MISSING_SCHEMA_CACHE_TTL_SEC,"30");
            originals.put(PRODUCER_MISSING_CACHE_SIZE,"60");
            originals.put(PRODUCER_BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS,"60");
            originals.put(PRODUCER_BEARER_AUTH_SCOPE_CLAIM_NAME,"test");
            originals.put(PRODUCER_BEARER_AUTH_SUB_CLAIM_NAME,"test2");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            Node node = new Node(1,"localhost",9092);
            PartitionInfo partitionInfo = new PartitionInfo("success",0,node,new Node[]{},new Node[]{});
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList(partitionInfo));
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new RoundRobinPartitioner(),new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertDoesNotThrow(()->publishConfigurer.configureProducerPublishMode(httpConnectorConfig, kafkaProducer));
        }

        @Test
        void test_bootstrap_servers_producer_format_and_content(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            originals.put("producer.format","json");
            originals.put("producer.content","response");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            Node node = new Node(1,"localhost",9092);
            PartitionInfo partitionInfo = new PartitionInfo("success",0,node,new Node[]{},new Node[]{});
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList(partitionInfo));
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new RoundRobinPartitioner(),new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertDoesNotThrow(()->publishConfigurer.configureProducerPublishMode(httpConnectorConfig, kafkaProducer));
        }

        @Test
        void test_only_bootstrap_servers_with_empty_partition_info(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            when(cluster.partitionsForTopic(anyString())).thenReturn(Lists.newArrayList());
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new RoundRobinPartitioner(),new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertThrows(IllegalStateException.class,()->publishConfigurer.configureProducerPublishMode(httpConnectorConfig, kafkaProducer));
        }
        @Test
        void test_only_bootstrap_servers_with_connectivity_error(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HashMap<String, String> originals = Maps.newHashMap();
            originals.put("producer.bootstrap.servers","localhost:9092");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(originals);
            Cluster cluster = mock(Cluster.class);
            when(cluster.partitionsForTopic(anyString())).thenThrow(new KafkaException());
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new RoundRobinPartitioner(),new StringSerializer(),null);
            KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(mockProducer);
            Assertions.assertThrows(KafkaException.class,()->publishConfigurer.configureProducerPublishMode(httpConnectorConfig, kafkaProducer));
        }
    }

    @Nested
    class ConfigureInMemoryQueue{

        @Test
        void test_null(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            Assertions.assertThrows(NullPointerException.class,()->publishConfigurer.configureInMemoryQueue(null));
        }
        @Test
        void test_empty_connector_config(){
            PublishConfigurer publishConfigurer = PublishConfigurer.build();
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(Maps.newHashMap());
            QueueFactory.registerConsumerForQueue(DEFAULT_QUEUE_NAME);
            Assertions.assertDoesNotThrow(()->publishConfigurer.configureInMemoryQueue(httpConnectorConfig));
        }
    }
}