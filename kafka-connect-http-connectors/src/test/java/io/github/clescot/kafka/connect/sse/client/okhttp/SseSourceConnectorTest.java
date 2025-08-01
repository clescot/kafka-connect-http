package io.github.clescot.kafka.connect.sse.client.okhttp;

import org.gradle.internal.impldep.com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class SseSourceConnectorTest {


    @Nested
    class Version{
        @Test
        void nominal_case(){
            SseSourceConnector sseSourceConnector = new SseSourceConnector();
            String version = sseSourceConnector.version();
            assertThat(version).isNotNull().isNotBlank();
        }
    }

    @Nested
    class TaskConfigs{
        private SseSourceConnector sseSourceConnector;
        @BeforeEach
        void setup(){
            sseSourceConnector = new SseSourceConnector();
            HashMap<String, String> settings = Maps.newHashMap();
            settings.put("config.default.topic","test");
            settings.put("config.default.url","http://localhost:8080/sse");
            sseSourceConnector.start(settings);
        }
        @Test
        void nominal_case(){
            Assertions.assertDoesNotThrow(()->sseSourceConnector.taskConfigs(1));
        }
        @Test
        void test_0_tasks(){
            Assertions.assertThrows(IllegalArgumentException.class,()->sseSourceConnector.taskConfigs(0));
        }

        @Test
        void test_negative_tasks(){
            Assertions.assertThrows(IllegalArgumentException.class,()->sseSourceConnector.taskConfigs(-1));
        }

        @Test
        void test_two_sse_endpoints_with_one_task(){
            HashMap<String, String> settings = Maps.newHashMap();
            settings.put("config.ids","default,second");
            settings.put("config.default.topic","test");
            settings.put("config.default.url","http://localhost:8080/sse");
            settings.put("config.second.topic","test2");
            settings.put("config.second.url","http://localhost:8080/sse2");
            sseSourceConnector.start(settings);
            var taskConfigs = sseSourceConnector.taskConfigs(1);
            assertThat(taskConfigs).hasSize(1);
            assertThat(taskConfigs.get(0)).containsEntry("config.default.url", "http://localhost:8080/sse");
            assertThat(taskConfigs.get(0)).containsEntry("config.default.topic","test");
            assertThat(taskConfigs.get(0)).containsEntry("config.second.url", "http://localhost:8080/sse2");
            assertThat(taskConfigs.get(0)).containsEntry("config.second.topic","test2");
        }
        @Test
        void test_two_sse_endpoints_with_two_task(){
            HashMap<String, String> settings = Maps.newHashMap();
            settings.put("config.ids","default,second");
            settings.put("config.default.topic","test");
            settings.put("config.default.url","http://localhost:8080/sse");
            settings.put("config.second.topic","test2");
            settings.put("config.second.url","http://localhost:8080/sse2");
            sseSourceConnector.start(settings);
            var taskConfigs = sseSourceConnector.taskConfigs(2);
            assertThat(taskConfigs).hasSize(2);
            //first task should have the default configuration
            assertThat(taskConfigs.get(0)).containsEntry("config.default.url", "http://localhost:8080/sse");
            assertThat(taskConfigs.get(0)).containsEntry("config.default.topic","test");
            assertThat(taskConfigs.get(0)).doesNotContainEntry("config.second.url", "http://localhost:8080/sse2");
            assertThat(taskConfigs.get(0)).doesNotContainEntry("config.second.topic","test2");

            //second task should have the second configuration
            assertThat(taskConfigs.get(1)).containsEntry("config.second.url", "http://localhost:8080/sse2");
            assertThat(taskConfigs.get(1)).containsEntry("config.second.topic","test2");
            assertThat(taskConfigs.get(1)).doesNotContainEntry("config.default.url", "http://localhost:8080/sse");
            assertThat(taskConfigs.get(1)).doesNotContainEntry("config.default.topic","test");
        }
    }

}