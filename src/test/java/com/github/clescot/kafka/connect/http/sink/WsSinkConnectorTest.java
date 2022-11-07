package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.sink.service.KafkaFailSafeProducer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.github.clescot.kafka.connect.http.source.AckConfig;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.connect.connector.Task;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.github.clescot.kafka.connect.http.sink.config.ConfigConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(Enclosed.class)
public class WsSinkConnectorTest {


    public static class Test_start{

        @Test
        public void test_with_empty_map(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            wsSinkConnector.start(Maps.newHashMap());
        }
        @Test(expected = NullPointerException.class)
        public void test_with_null_map(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            wsSinkConnector.start(null);
        }

        @Test
        public void test_with_nominal_case(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            settings.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            settings.put(PRODUCER_CLIENT_ID,"fake.client.id");
            settings.put(ACK_TOPIC,"fake.ack.topic");
            settings.put(ACK_SCHEMA,"{\n" +
                    "    \"namespace\": \"com.fake.namespace\",\n" +
                    "    \"name\": \"Test\",\n" +
                    "    \"doc\": \"test doc\",\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"fields\": [\n" +
                    "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                    "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                    "    ]\n" +
                    "}");
            wsSinkConnector.start(settings);
        }

    }

    public static class Test_task_class{
        @Test
        public void test_nominal_case(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            Class<? extends Task> aClass = wsSinkConnector.taskClass();
            assertThat(aClass).isEqualTo(WsSinkTask.class);
        }
    }

    public static class Test_taskConfigs{
        @Test
        public void test_nominal_case(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            settings.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            settings.put(PRODUCER_CLIENT_ID,"fake.client.id");
            settings.put(ACK_TOPIC,"fake.ack.topic");
            settings.put(ACK_SCHEMA,"{\n" +
                    "    \"namespace\": \"com.fake.namespace\",\n" +
                    "    \"name\": \"Test\",\n" +
                    "    \"doc\": \"test doc\",\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"fields\": [\n" +
                    "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                    "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                    "    ]\n" +
                    "}");
            wsSinkConnector.start(settings);
            List<Map<String, String>> maps = wsSinkConnector.taskConfigs(1);
            assertThat(maps.size()).isEqualTo(1);
            assertThat(maps.get(0)).isEqualTo(settings);
        }

        @Test
        public void test_2_tasks(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            settings.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            settings.put(PRODUCER_CLIENT_ID,"fake.client.id");
            settings.put(ACK_TOPIC,"fake.ack.topic");
            settings.put(ACK_SCHEMA,"{\n" +
                    "    \"namespace\": \"com.fake.namespace\",\n" +
                    "    \"name\": \"Test\",\n" +
                    "    \"doc\": \"test doc\",\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"fields\": [\n" +
                    "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                    "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                    "    ]\n" +
                    "}");
            wsSinkConnector.start(settings);
            List<Map<String, String>> maps = wsSinkConnector.taskConfigs(2);
            assertThat(maps.size()).isEqualTo(2);
            assertThat(maps.get(0)).isEqualTo(settings);
            assertThat(maps.get(1)).isEqualTo(settings);
        }
    }

    public static class Test_stop{

        @Test
        public void test_nominal_case_without_ack_sender_already_initialized(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            settings.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            settings.put(PRODUCER_CLIENT_ID,"fake.client.id");
            settings.put(ACK_TOPIC,"fake.ack.topic");
            settings.put(ACK_SCHEMA,"{\n" +
                    "    \"namespace\": \"com.fake.namespace\",\n" +
                    "    \"name\": \"Test\",\n" +
                    "    \"doc\": \"test doc\",\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"fields\": [\n" +
                    "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                    "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                    "    ]\n" +
                    "}");
            wsSinkConnector.start(settings);
            wsSinkConnector.stop();
        }

    }
}