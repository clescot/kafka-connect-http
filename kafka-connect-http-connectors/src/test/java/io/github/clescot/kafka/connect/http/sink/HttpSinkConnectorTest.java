package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Maps;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class HttpSinkConnectorTest {

    @Nested
    class TestStart {
        @Test
        void test_start_with_empty_map() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Assertions.assertDoesNotThrow(() -> httpSinkConnector.start(Maps.newHashMap()));
        }

        @Test
        void test_start_with_null_map() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Assertions.assertThrows(NullPointerException.class, () -> httpSinkConnector.start(null));
        }

        @Test
        void test_start_with_nominal_case() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.httpclient.ssl.truststore.always.trust", "true");
            Assertions.assertDoesNotThrow(() -> httpSinkConnector.start(settings));
        }
    }

    @Nested
    class TestTaskClass {

        @Test
        void test_task_class_nominal_case() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Class<? extends Task> aClass = httpSinkConnector.taskClass();
            assertThat(aClass).isEqualTo(HttpSinkTask.class);
        }

    }

    @Nested
    class TestTaskConfig {
        @Test
        void test_taskConfigs_nominal_case() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            httpSinkConnector.start(settings);
            List<Map<String, String>> maps = httpSinkConnector.taskConfigs(1);
            assertThat(maps).hasSize(1);
            assertThat(maps.get(0)).isEqualTo(settings);
        }


        @Test
        void test_calling_task_configs_but_not_start() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Assertions.assertThrows(NullPointerException.class, () -> {
                        httpSinkConnector.taskConfigs(1);
                    }
            );

        }


        @Test
        void test_2_tasks() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            httpSinkConnector.start(settings);
            List<Map<String, String>> maps = httpSinkConnector.taskConfigs(2);
            assertThat(maps.size()).isEqualTo(2);
            assertThat(maps.get(0)).isEqualTo(settings);
            assertThat(maps.get(1)).isEqualTo(settings);
        }
    }

    @Nested
    class TestStop {
        @Test
        void test_stop_nominal_case_without_ack_sender_already_initialized() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            httpSinkConnector.start(settings);
            Assertions.assertDoesNotThrow(httpSinkConnector::stop);
        }

    }


}