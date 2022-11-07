package com.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Maps;
import org.apache.kafka.connect.connector.Task;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.github.clescot.kafka.connect.http.sink.config.ConfigConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

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
            settings.put(SUCCESS_TOPIC,"fake.ack.topic");

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
            settings.put(SUCCESS_TOPIC,"fake.success.topic");
            settings.put(ERRORS_TOPIC,"fake.errors.topic");
            wsSinkConnector.start(settings);
            List<Map<String, String>> maps = wsSinkConnector.taskConfigs(1);
            assertThat(maps.size()).isEqualTo(1);
            assertThat(maps.get(0)).isEqualTo(settings);
        }


        @Test
        public void test_calling_task_configs_but_not_start(){
            Assertions.assertThrows(NullPointerException.class,()->{
                    WsSinkConnector wsSinkConnector = new WsSinkConnector();
                    wsSinkConnector.taskConfigs(1);
                }
            );

        }


        @Test
        public void test_2_tasks(){
            WsSinkConnector wsSinkConnector = new WsSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(SUCCESS_TOPIC,"fake.ack.topic");
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
            settings.put(SUCCESS_TOPIC,"fake.ack.topic");
            wsSinkConnector.start(settings);
            wsSinkConnector.stop();
        }

    }
}