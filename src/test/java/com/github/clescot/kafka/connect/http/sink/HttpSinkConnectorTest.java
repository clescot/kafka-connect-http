package com.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Maps;
import org.apache.kafka.connect.connector.Task;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Enclosed.class)
public class HttpSinkConnectorTest {


    public static class Test_start{

        @Test
        public void test_with_empty_map(){
            HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
            wsSinkConnector.start(Maps.newHashMap());
        }
        @Test(expected = NullPointerException.class)
        public void test_with_null_map(){
            HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
            wsSinkConnector.start(null);
        }

        @Test
        public void test_with_nominal_case(){
            HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            wsSinkConnector.start(settings);
        }

    }

    public static class Test_task_class{
        @Test
        public void test_nominal_case(){
            HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
            Class<? extends Task> aClass = wsSinkConnector.taskClass();
            assertThat(aClass).isEqualTo(HttpSinkTask.class);
        }
    }

    public static class Test_taskConfigs{
        @Test
        public void test_nominal_case(){
            HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            wsSinkConnector.start(settings);
            List<Map<String, String>> maps = wsSinkConnector.taskConfigs(1);
            assertThat(maps.size()).isEqualTo(1);
            assertThat(maps.get(0)).isEqualTo(settings);
        }


        @Test
        public void test_calling_task_configs_but_not_start(){
            Assertions.assertThrows(NullPointerException.class,()->{
                    HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
                    wsSinkConnector.taskConfigs(1);
                }
            );

        }


        @Test
        public void test_2_tasks(){
            HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
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
            HttpSinkConnector wsSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            wsSinkConnector.start(settings);
            wsSinkConnector.stop();
        }

    }
}