package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.ConfigConstants;
import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;

import static com.github.clescot.kafka.connect.http.source.WsSourceConfigDefinition.ERROR_TOPIC;
import static com.github.clescot.kafka.connect.http.source.WsSourceConfigDefinition.SUCCESS_TOPIC;


public class WsSourceConnectorConfigTest {



    @Test(expected = NullPointerException.class)
    public void test_null_map(){
        new WsSourceConnectorConfig(null);
    }
    @Test(expected = ConfigException.class)
    public void test_empty_map(){
        new WsSourceConnectorConfig(Maps.newHashMap());
    }

    @Test
    public void test_nominal_case(){
        HashMap<Object, Object> config = Maps.newHashMap();
        config.put(SUCCESS_TOPIC,"success.topic");
        config.put(ERROR_TOPIC,"error.topic");
        new WsSourceConnectorConfig(config);
    }




    @Test(expected = ConfigException.class)
    public void test_missing_ack_topic(){
        HashMap<Object, Object> config = Maps.newHashMap();
        new WsSourceConnectorConfig(config);
    }


}