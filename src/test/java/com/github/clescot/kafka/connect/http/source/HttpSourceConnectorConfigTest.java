package com.github.clescot.kafka.connect.http.source;

import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;

import static com.github.clescot.kafka.connect.http.source.HttpSourceConfigDefinition.ERROR_TOPIC;
import static com.github.clescot.kafka.connect.http.source.HttpSourceConfigDefinition.SUCCESS_TOPIC;


public class HttpSourceConnectorConfigTest {



    @Test(expected = NullPointerException.class)
    public void test_null_map(){
        new HttpSourceConnectorConfig(null);
    }
    @Test(expected = ConfigException.class)
    public void test_empty_map(){
        new HttpSourceConnectorConfig(Maps.newHashMap());
    }

    @Test
    public void test_nominal_case(){
        HashMap<Object, Object> config = Maps.newHashMap();
        config.put(SUCCESS_TOPIC,"success.topic");
        config.put(ERROR_TOPIC,"error.topic");
        new HttpSourceConnectorConfig(config);
    }




    @Test(expected = ConfigException.class)
    public void test_missing_ack_topic(){
        HashMap<Object, Object> config = Maps.newHashMap();
        new HttpSourceConnectorConfig(config);
    }


}