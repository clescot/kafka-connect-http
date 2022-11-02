package com.github.clescot.kafka.connect.http.source;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class WsSourceConnectorTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(WsSourceConnectorTest.class);
    private WsSourceConnector wsSourceConnector;

    @BeforeEach
    public void setup(){
        wsSourceConnector = new WsSourceConnector();
    }

    @Test
    public void test_start_nominal_case(){
        Map<String,String> settings = Maps.newHashMap();
        settings.put("ack.topic","foo");
        wsSourceConnector.start(settings);
    }

    @Test
    public void test_start_empty_settings_map(){
        Map<String,String> settings = Maps.newHashMap();
        Assertions.assertThrows(IllegalArgumentException.class, () -> wsSourceConnector.start(settings));
    }

    @Test
    public void test_start_null_settings_map(){
        Assertions.assertThrows(NullPointerException.class, () -> wsSourceConnector.start(null));
    }

    @Test
    public void test_task_configs_zero_task(){
        Map<String,String> settings = Maps.newHashMap();
        settings.put("ack.topic","foo");
        wsSourceConnector.start(settings);
        List<Map<String, String>> maps = wsSourceConnector.taskConfigs(0);
        assertThat(maps).asList().isEmpty();
    }

    @Test
    public void test_task_configs_1_task(){
        Map<String,String> settings = Maps.newHashMap();
        settings.put("ack.topic","foo");
        wsSourceConnector.start(settings);
        List<Map<String, String>> maps = wsSourceConnector.taskConfigs(1);
        assertThat(maps).asList().hasSize(1);

    }
      @Test
    public void test_task_configs_10_tasks(){
        Map<String,String> settings = Maps.newHashMap();
        settings.put("ack.topic","foo");
        wsSourceConnector.start(settings);
          List<Map<String, String>> maps = wsSourceConnector.taskConfigs(10);
          assertThat(maps).asList().hasSize(10);
      }


}