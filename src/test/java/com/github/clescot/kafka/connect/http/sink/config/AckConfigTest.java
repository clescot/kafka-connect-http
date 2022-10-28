package com.github.clescot.kafka.connect.http.sink.config;

import com.github.clescot.kafka.connect.http.source.AckConfig;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;


public class AckConfigTest {

    public static final String DEFAULT_SINK_PRODUCER_ID = "httpSinkProducer";

    @Test(expected = NullPointerException.class)
    public void test_null_map(){
        new AckConfig(null);
    }
    @Test(expected = IllegalArgumentException.class)
    public void test_empty_map(){
        new AckConfig(Maps.newHashMap());
    }

    @Test
    public void test_nominal_case(){
        HashMap<Object, Object> config = Maps.newHashMap();
        config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER,"fake.bootstrap.servers.com:9092");
        config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY,"fake.schema.registry:8081");
        config.put(ConfigConstants.PRODUCER_CLIENT_ID,"fake.client.id");
        config.put(ConfigConstants.ACK_TOPIC,"fake.ack.topic");
        config.put(ConfigConstants.ACK_SCHEMA,"{\n" +
                "    \"namespace\": \"com.fake.namespace\",\n" +
                "    \"name\": \"Test\",\n" +
                "    \"doc\": \"test doc\",\n" +
                "    \"type\": \"record\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                "    ]\n" +
                "}");
        new AckConfig(config);
    }




    @Test(expected = IllegalArgumentException.class)
    public void test_missing_ack_topic(){
        HashMap<Object, Object> config = Maps.newHashMap();
        config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER,"fake.bootstrap.servers.com:9092");
        config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY,"fake.schema.registry:8081");
        config.put(ConfigConstants.PRODUCER_CLIENT_ID,"fake.client.id");
        config.put(ConfigConstants.ACK_SCHEMA,"{\n" +
                "    \"namespace\": \"com.fake.namespace\",\n" +
                "    \"name\": \"Test\",\n" +
                "    \"doc\": \"test doc\",\n" +
                "    \"type\": \"record\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                "    ]\n" +
                "}");
        new AckConfig(config);
    }


}