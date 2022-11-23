package com.github.clescot.kafka.connect.http.source;

import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.github.clescot.kafka.connect.http.ConfigConstants.QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.QueueFactory.DEFAULT_QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.source.HttpSourceConfigDefinition.ERROR_TOPIC;
import static com.github.clescot.kafka.connect.http.source.HttpSourceConfigDefinition.SUCCESS_TOPIC;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class HttpSourceConnectorTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpSourceConnectorTest.class);
    private HttpSourceConnector wsSourceConnector;

    @BeforeEach
    public void setup(){
        wsSourceConnector = new HttpSourceConnector();
    }

    @Test
    public void test_start_nominal_case(){
        Map<String,String> settings = Maps.newHashMap();
        settings.put(SUCCESS_TOPIC,"foo");
        settings.put(ERROR_TOPIC,"foo");
        wsSourceConnector.start(settings);
    }

    @Test
    public void test_start_missing_success_topic(){
        Assertions.assertThrows(ConfigException.class, () ->  {
                Map < String, String > settings = Maps.newHashMap();
                settings.put(ERROR_TOPIC, "foo");
                wsSourceConnector.start(settings);
        });
    }

    @Test
    public void test_start_missing_errors_topic(){
        Assertions.assertThrows(ConfigException.class, () ->  {
                Map < String, String > settings = Maps.newHashMap();
                settings.put(SUCCESS_TOPIC, "foo");
                wsSourceConnector.start(settings);
        });
    }

    @Test
    public void test_start_with_queue_name(){
            Map < String, String > settings = Maps.newHashMap();
            settings.put(SUCCESS_TOPIC, "foo1");
            settings.put(ERROR_TOPIC, "foo2");
            settings.put(QUEUE_NAME, "myQueue");
            wsSourceConnector.start(settings);
    }

    @Test
    public void test_start_with_default_queue_name(){
        Map < String, String > settings = Maps.newHashMap();
        settings.put(SUCCESS_TOPIC, "foo1");
        settings.put(ERROR_TOPIC, "foo2");
        settings.put(QUEUE_NAME, DEFAULT_QUEUE_NAME);
        wsSourceConnector.start(settings);
    }



    @Test
    public void test_start_empty_settings_map(){
        Map<String,String> settings = Maps.newHashMap();
        Assertions.assertThrows(ConfigException.class, () -> wsSourceConnector.start(settings));
    }

    @Test
    public void test_start_null_settings_map(){
        Assertions.assertThrows(NullPointerException.class, () -> wsSourceConnector.start(null));
    }

    @Test
    public void test_task_configs_zero_task(){
        Map<String,String> settings = Maps.newHashMap();
        settings.put(SUCCESS_TOPIC,"foo");
        settings.put(ERROR_TOPIC,"foo");
        wsSourceConnector.start(settings);
        List<Map<String, String>> maps = wsSourceConnector.taskConfigs(0);
        assertThat(maps).asList().isEmpty();
    }

    @Test
    public void test_task_configs_1_task(){
        Map<String,String> settings = Maps.newHashMap();
        settings.put(SUCCESS_TOPIC,"foo");
        settings.put(ERROR_TOPIC,"foo");
        wsSourceConnector.start(settings);
        List<Map<String, String>> maps = wsSourceConnector.taskConfigs(1);
        assertThat(maps).asList().hasSize(1);

    }
      @Test
    public void test_task_configs_10_tasks(){
        Map<String,String> settings = Maps.newHashMap();
          settings.put(SUCCESS_TOPIC,"foo");
          settings.put(ERROR_TOPIC,"foo");
        wsSourceConnector.start(settings);
          List<Map<String, String>> maps = wsSourceConnector.taskConfigs(10);
          assertThat(maps).asList().hasSize(10);
      }


}