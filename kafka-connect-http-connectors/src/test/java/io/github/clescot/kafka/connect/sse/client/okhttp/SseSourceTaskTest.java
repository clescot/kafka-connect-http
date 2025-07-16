package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.quartz.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SseSourceTaskTest {
    @Nested
    class Start {

        SseSourceTask sseSourceTask;

        @BeforeEach
        void setup() {
            sseSourceTask = new SseSourceTask();
        }


        @Test
        void test_empty_settings() {
            HashMap<String, String> settings = Maps.newHashMap();
            Assertions.assertThrows(ConfigException.class, () -> sseSourceTask.start(settings));
        }

        @Test
        void test_null_settings() {
            Assertions.assertThrows(NullPointerException.class, () -> sseSourceTask.start(null));
        }

        @Test
        void test_settings_with_topic_only() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            Assertions.assertThrows(ConfigException.class, () -> sseSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_url() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("url", "http://localhost:8080/sse");
            Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
        }




        @AfterEach
        void shutdown() {
            sseSourceTask.stop();
        }
    }


    @Nested
    class Version {

        @Test
        void get_version() {
            SseSourceTask SseSourceTask = new SseSourceTask();
            String version = SseSourceTask.version();
            assertThat(version)
                    .isNotNull()
                    .isNotBlank();
        }
    }

    @Nested
    class Poll{
        SseSourceTask SseSourceTask;

        @BeforeEach
        void setup() {
            SseSourceTask = new SseSourceTask();
        }

        @AfterEach
        void shutdown() {
            SseSourceTask.stop();
        }

        @Test
        void test_nominal_case() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("configuration.id", "test_sse_client_connect");
            settings.put("topic", "test");
            settings.put("url", "http://localhost:8080/sse");
            SseSourceTask.start(settings);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()-> SseSourceTask.getQueue().peek()!=null);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()->!SseSourceTask.poll().isEmpty());

        }
    }

}