package io.github.clescot.kafka.connect.sse.client.okhttp;


import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SseConfigurationTest {

    @Nested
    class Constructor {

        @Test
        void nominal_case() {
            HttpClientConfiguration<OkHttpClient, Request, Response> configuration = new HttpClientConfiguration<>(
                    "test-id",
                    new OkHttpClientFactory(),
                    Map.of("url", "http://example.com/sse", "topic", "test-topic"),
                    null,
                    new CompositeMeterRegistry()
            );
            HashMap<String, Object> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            assertThat(sseConfiguration.getConfigurationId()).isEqualTo("test-id");
        }

        @Test
        void null_args() {
            Assertions.assertThrows(NullPointerException.class,()->new SseConfiguration(null,null,null));
        }

        @Test
        void null_args_with_configuration_id_set() {
            Assertions.assertThrows(NullPointerException.class,()->new SseConfiguration("test",null,null));
        }
        @Test
        void null_args_with_configuration_id__and_http_client_configuration_set() {
            HttpClientConfiguration<OkHttpClient, Request, Response> configuration = new HttpClientConfiguration<>(
                    "test-id",
                    new OkHttpClientFactory(),
                    Map.of("url", "http://example.com/sse", "topic", "test-topic"),
                    null,
                    new CompositeMeterRegistry()
            );
            Assertions.assertThrows(NullPointerException.class,()->new SseConfiguration("test",configuration,null));
        }

        @Test
        void empty_settings() {
            HttpClientConfiguration<OkHttpClient, Request, Response> configuration = new HttpClientConfiguration<>(
                    "test-id",
                    new OkHttpClientFactory(),
                    Map.of("url", "http://example.com/sse", "topic", "test-topic"),
                    null,
                    new CompositeMeterRegistry()
            );
            HashMap<String, Object> emptySettings = Maps.newHashMap();
            Assertions.assertThrows(IllegalArgumentException.class,()->new SseConfiguration("test",configuration, emptySettings));
        }
    }

}