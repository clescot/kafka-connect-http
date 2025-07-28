package io.github.clescot.kafka.connect.sse.client.okhttp;


import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.ErrorStrategy;
import com.launchdarkly.eventsource.StreamHttpErrorException;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
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
            Assertions.assertThrows(NullPointerException.class, () -> new SseConfiguration(null, null, null));
        }

        @Test
        void null_args_with_configuration_id_set() {
            Assertions.assertThrows(NullPointerException.class, () -> new SseConfiguration("test", null, null));
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
            Assertions.assertThrows(NullPointerException.class, () -> new SseConfiguration("test", configuration, null));
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
            Assertions.assertThrows(IllegalArgumentException.class, () -> new SseConfiguration("test", configuration, emptySettings));
        }
    }

    @Nested
    class BuildSseConfiguration {
        @Test
        void nominal_case() {
            Map<String, Object> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = SseConfiguration.buildSseConfiguration("test-id", settings);
            assertThat(sseConfiguration.getConfigurationId()).isEqualTo("test-id");
            assertThat(sseConfiguration.getUri().toString()).hasToString("http://example.com/sse");
            assertThat(sseConfiguration.getTopic()).isEqualTo("test-topic");
        }

        @Test
        void null_settings() {
            Assertions.assertThrows(NullPointerException.class, () -> SseConfiguration.buildSseConfiguration("test-id", null));
        }

        @Test
        void empty_settings() {
            HashMap<String, Object> emptySettings = Maps.newHashMap();
            Assertions.assertThrows(IllegalArgumentException.class, () -> SseConfiguration.buildSseConfiguration("test-id", emptySettings));
        }
    }

    @Nested
    class Connect {
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
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getQueue()).isNotNull();
        }

        @Test
        void null_queue() {
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
            Assertions.assertThrows(NullPointerException.class, () -> sseConfiguration.connect(null));
        }

        @Test
        void connect_with_retry_strategy() {
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
            settings.put("retry.delay.strategy.max-delay-millis", 5000L);
            settings.put("retry.delay.strategy.backoff-multiplier", 1.5F);
            settings.put("retry.delay.strategy.jitter-multiplier", 0.4F);
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
        }

        @Test
        void connect_without_error_strategy_set() {
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
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
        }

        @Test
        void connect_with_error_strategy_always_continue() {
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
            settings.put("error.strategy", "always-continue");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
        }

        @Test
        void connect_with_error_strategy_always_throw() {
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
            settings.put("error.strategy", "always-throw");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
        }

        @Test
        void connect_with_unknown_error_strategy() {
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
            settings.put("error.strategy", "dummy");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            try(BackgroundEventSource ignored = sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
                assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
                ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
                ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
                assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
            }
        }

        @Test
        void connect_with_error_strategy_continue_with_max_attempts() {
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
            settings.put("error.strategy", "continue-with-max-attempts");
            settings.put("error.strategy.max-attempts", 4);
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            ErrorStrategy.Result result2 = result.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result2.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            ErrorStrategy.Result result3 = result2.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result3.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            ErrorStrategy.Result result4 = result3.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result4.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            ErrorStrategy.Result result5 = result4.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result5.getAction()).isEqualTo(ErrorStrategy.Action.THROW);

        }

        @Test
        void connect_with_error_strategy_continue_with_max_attempts_without_max_attempts() {
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
            settings.put("error.strategy", "continue-with-max-attempts");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            ErrorStrategy.Result result2 = result.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result2.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            ErrorStrategy.Result result3 = result2.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result3.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            ErrorStrategy.Result result4 = result3.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result4.getAction()).isEqualTo(ErrorStrategy.Action.THROW);

        }

        @Test
        void connect_with_error_strategy_continue_with_time_limit() throws InterruptedException {
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
            settings.put("error.strategy", "continue-with-time-limit");
            settings.put("error.strategy.time-limit-count-in-millis", 5000L);
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            // Simulate the time limit being reached
            Thread.sleep(5000);
            // After the time limit, it should throw an exception
            ErrorStrategy.Result result2 = result.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result2.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
        }


    }

    @Nested
    class Connect_with_high_execution_time{
        @Test
        @SuppressWarnings("java:s2925")
        void connect_with_error_strategy_continue_with_time_limit_without_time_limit() throws InterruptedException {
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
            settings.put("error.strategy", "continue-with-time-limit");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", configuration, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            Thread.sleep(5000);
            // Test the time limit being reached
            ErrorStrategy.Result result2 = result.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result2.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
            Thread.sleep(60000L); // Wait for a long time to ensure no further actions are taken
            // After the time limit, it should throw an exception
            ErrorStrategy.Result result3 = result2.getNext().apply(new StreamHttpErrorException(500));
            assertThat(result3.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
        }
    }

    @Nested
    class Start {
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
            try(BackgroundEventSource ignored = sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
                sseConfiguration.start();
                assertThat(sseConfiguration.isStarted()).isTrue();
            }
        }

        @Test
        void not_connected() {
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
            Assertions.assertThrows(IllegalStateException.class, sseConfiguration::start);
        }
    }

}