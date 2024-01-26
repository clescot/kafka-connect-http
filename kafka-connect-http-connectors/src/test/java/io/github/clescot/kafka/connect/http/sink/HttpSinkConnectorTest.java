package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
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
            assertThat(maps).hasSize(2);
            assertThat(maps.get(0)).isEqualTo(settings);
            assertThat(maps.get(1)).isEqualTo(settings);
        }

        @Test
        void test_with_dynamic_parameters() {
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.enrich.request.static.header.names","header1,header2");
            settings.put("config.default.enrich.request.static.header.header1","value1");
            settings.put("config.default.enrich.request.static.header.header2","value2");
            httpSinkConnector.start(settings);
            List<Map<String, String>> maps = httpSinkConnector.taskConfigs(2);
            assertThat(maps).hasSize(2);
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

    @Nested
    class TestConfig {
        @Test
        void test_nominal_case(){
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            httpSinkConnector.start(settings);
            ConfigDef configDef = httpSinkConnector.config();
            assertThat(configDef).isNotNull();
        }
        @Test
        void test_with_default_configuration_and_custom_static_headers(){

            //given
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.enrich.request.static.header.names","header1,header2");
            settings.put("config.default.enrich.request.static.header.header1","value1");
            settings.put("config.default.enrich.request.static.header.header2","value2");
            httpSinkConnector.start(settings);

            //when
            ConfigDef configDef = httpSinkConnector.config();

            //then
            assertThat(configDef).isNotNull();
            assertThat(configDef.configKeys()).containsKey("config.default.enrich.request.static.header.header1");
        }
        @Test
        void test_with_multiple_configurations_and_custom_static_headers(){

            //given
            HttpSinkConnector httpSinkConnector = new HttpSinkConnector();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.ids","config1,config2");
            settings.put("config.default.enrich.request.static.header.names","header1,header2");
            settings.put("config.default.enrich.request.static.header.header1","value1");
            settings.put("config.default.enrich.request.static.header.header2","value2");
            settings.put("config.config1.enrich.request.static.header.names","header3,header4");
            settings.put("config.config1.enrich.request.static.header.header1","value1");
            settings.put("config.config1.enrich.request.static.header.header2","value2");
            settings.put("config.config2.enrich.request.static.header.names","header5,header6");
            settings.put("config.config2.enrich.request.static.header.header1","value1");
            settings.put("config.config2.enrich.request.static.header.header2","value2");
            httpSinkConnector.start(settings);

            //when
            ConfigDef configDef = httpSinkConnector.config();

            //then
            assertThat(configDef).isNotNull();
            assertThat(configDef.configKeys()).containsKey("config.default.enrich.request.static.header.header1");
            assertThat(configDef.configKeys()).containsKey("config.default.enrich.request.static.header.header2");
            assertThat(configDef.configKeys()).containsKey("config.config1.enrich.request.static.header.header3");
            assertThat(configDef.configKeys()).containsKey("config.config1.enrich.request.static.header.header4");
            assertThat(configDef.configKeys()).containsKey("config.config2.enrich.request.static.header.header5");
            assertThat(configDef.configKeys()).containsKey("config.config2.enrich.request.static.header.header6");
        }
    }
}
