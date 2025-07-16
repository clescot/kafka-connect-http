package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector;
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
            settings.put("topic","test");
            settings.put("url","http://localhost:8080/sse");
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
    }

}