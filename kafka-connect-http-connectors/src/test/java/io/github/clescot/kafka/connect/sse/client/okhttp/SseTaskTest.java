package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;


class SseTaskTest {

    @Nested
    class Constructor {

        @Test
        void null_args() {
            assertThrows(NullPointerException.class, () -> new SseTask(null));
        }
        @Test
        void empty_settings() {
            HashMap<String, String> emptySettings = Maps.newHashMap();
            assertThrows(ConfigException.class, () -> new SseTask(emptySettings));
        }

        @Test
        void valid_args() {
            SseTask sseTask = new SseTask(
                    Map.of("config.default.url", "http://localhost:8080/sse",
                            "config.default.topic", "test-topic"
                    )
            );
            assertNotNull(sseTask);
        }
    }

    @Nested
    class Connect {

        @Test
        void connect() {
            SseTask sseTask = new SseTask(
                    Map.of("config.default.url", "http://localhost:8080/sse",
                            "config.default.topic", "test-topic"
                    )
            );
            sseTask.connect();
            assertThat(sseTask.getDefaultConfiguration().getConfigurationId()).isEqualTo("default");
            assertThat(sseTask.getDefaultTopic()).isEqualTo("test-topic");
            assertThat(sseTask.isConnected("default")).isTrue();
            assertThat(sseTask.isStarted("default")).isFalse();
            assertFalse(sseTask.getQueues().isEmpty());

        }
    }


    @Nested
    class Start {

        @Test
        void start() {
            SseTask sseTask = new SseTask(
                    Map.of("config.default.url", "http://localhost:8080/sse",
                            "config.default.topic", "test-topic"
                    )
            );
            sseTask.connect();
            sseTask.start();
            assertThat(sseTask.isStarted("default")).isTrue();
        }

        @Test
        void start_not_connected() {
            SseTask sseTask = new SseTask(
                    Map.of("config.default.url", "http://localhost:8080/sse",
                            "config.default.topic", "test-topic"
                    )
            );
            sseTask.start();
            assertThat(sseTask.isStarted("default")).isTrue();
        }
    }


    @Nested
    class Stop {

        @Test
        void connect_start_and_stop() {
            SseTask sseTask = new SseTask(
                    Map.of("config.default.url", "http://localhost:8080/sse",
                            "config.default.topic", "test-topic"
                    )
            );
            sseTask.connect();
            sseTask.start();
            sseTask.stop();
            assertThat(sseTask.isStarted("default")).isFalse();
            assertThat(sseTask.isConnected("default")).isFalse();
        }

        @Test
        void stop_without_connect_and_start() {
            SseTask sseTask = new SseTask(
                    Map.of("config.default.url", "http://localhost:8080/sse",
                            "config.default.topic", "test-topic"
                    )
            );
            sseTask.stop();
            assertThat(sseTask.isStarted("default")).isFalse();
            assertThat(sseTask.isConnected("default")).isFalse();
        }
    }
}