package com.github.clescot.kafka.connect.http.sink.config;

import com.github.clescot.kafka.connect.http.source.SourceConfig;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;


public class AckConfigTest {



    @Test(expected = NullPointerException.class)
    public void test_null_map(){
        new SourceConfig(null);
    }
    @Test(expected = IllegalArgumentException.class)
    public void test_empty_map(){
        new SourceConfig(Maps.newHashMap());
    }

    @Test
    public void test_nominal_case(){
        HashMap<Object, Object> config = Maps.newHashMap();
        config.put(ConfigConstants.SUCCESS_TOPIC,"success.topic");
        config.put(ConfigConstants.ERRORS_TOPIC,"errors.topic");
        new SourceConfig(config);
    }




    @Test(expected = IllegalArgumentException.class)
    public void test_missing_ack_topic(){
        HashMap<Object, Object> config = Maps.newHashMap();
        new SourceConfig(config);
    }


}