package io.github.clescot.kafka.connect.sse.client.okhttp;


import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.ErrorStrategy;
import com.launchdarkly.eventsource.StreamHttpErrorException;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class SseConfigurationTest {

    @Nested
    class Constructor {

        @Test
        void nominal_case() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            assertThat(sseConfiguration.getConfigurationId()).isEqualTo("test-id");
            assertThat(sseConfiguration.getUri().toString()).hasToString("http://example.com/sse");
            assertThat(sseConfiguration.getTopic()).isEqualTo("test-topic");
            //error strategy, retry delay strategy and connect strategy should be null by default at constructor
            //they are built when connect is called
            assertThat(sseConfiguration.getErrorStrategy()).isNull();
            assertThat(sseConfiguration.getRetryDelayStrategy()).isNull();
            assertThat(sseConfiguration.getConnectStrategy()).isNull();
            assertThat(sseConfiguration.getBackgroundEventHandler()).isNull();
            assertThat(sseConfiguration.matches(new HttpRequest("http://localhost:8080"))).isFalse();
            assertThat(sseConfiguration.getClient()).isNotNull();
            assertThat(sseConfiguration.getSettings()).isEqualTo(settings);
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
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            Assertions.assertThrows(NullPointerException.class, () -> new SseConfiguration("test", okHttpClient, null));
        }

        @Test
        void empty_settings() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            HashMap<String, String> emptySettings = Maps.newHashMap();
            Assertions.assertThrows(IllegalArgumentException.class, () -> new SseConfiguration("test", okHttpClient, emptySettings));
        }
    }

    @Nested
    class BuildSseConfiguration {
        @Test
        void nominal_case() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            SseConfiguration sseConfiguration = SseConfiguration.buildSseConfiguration("test-id", settings, null, new CompositeMeterRegistry(), okHttpClientFactory);
            assertThat(sseConfiguration.getConfigurationId()).isEqualTo("test-id");
            assertThat(sseConfiguration.getUri().toString()).hasToString("http://example.com/sse");
            assertThat(sseConfiguration.getTopic()).isEqualTo("test-topic");
        }

        @Test
        void null_settings() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Assertions.assertThrows(NullPointerException.class, () -> SseConfiguration.buildSseConfiguration("test-id", null, null, new CompositeMeterRegistry(), okHttpClientFactory));
        }

        @Test
        void empty_settings() {
            HashMap<String, String> emptySettings = Maps.newHashMap();
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Assertions.assertThrows(IllegalArgumentException.class, () -> SseConfiguration.buildSseConfiguration("test-id", emptySettings, null, new CompositeMeterRegistry(), okHttpClientFactory));
        }
    }

    @Nested
    class Connect {
        @Test
        void nominal_case() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getQueue()).isNotNull();
        }

        @Test
        void null_queue() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse);");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            Assertions.assertThrows(NullPointerException.class, () -> sseConfiguration.connect(null));
        }

        @Test
        void connect_with_retry_strategy() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("retry.delay.strategy.max-delay-millis", "5000");
            settings.put("retry.delay.strategy.backoff-multiplier", "1.5");
            settings.put("retry.delay.strategy.jitter-multiplier", "0.4");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            assertThat(sseConfiguration.getRetryDelayStrategy()).isNotNull();
        }

        @Test
        void connect_with_retry_error_and_connect_strategy() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("retry.delay.strategy.max-delay-millis", "5000");
            settings.put("retry.delay.strategy.backoff-multiplier", "1.5");
            settings.put("retry.delay.strategy.jitter-multiplier", "0.4");
            settings.put("error.strategy", "always-throw");
            settings.put("okhttp.connect.timeout", "3000");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            assertThat(sseConfiguration.getConnectStrategy()).isNotNull();
            assertThat(sseConfiguration.getErrorStrategy()).isNotNull();
            assertThat(sseConfiguration.getRetryDelayStrategy()).isNotNull();
        }

        @Test
        void connect_without_error_strategy_set() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
        }

        @Test
        void connect_with_error_strategy_always_continue() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("error.strategy", "always-continue");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.CONTINUE);
        }

        @Test
        void connect_with_error_strategy_always_throw() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("error.strategy", "always-throw");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME));
            assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
            ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
            ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
            assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
        }

        @Test
        void connect_with_unknown_error_strategy() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("error.strategy", "dummy");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            try (BackgroundEventSource ignored = sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
                assertThat(sseConfiguration.getBackgroundEventSource()).isNotNull();
                ErrorStrategy errorStrategy = sseConfiguration.getErrorStrategy();
                ErrorStrategy.Result result = errorStrategy.apply(new StreamHttpErrorException(500));
                assertThat(result.getAction()).isEqualTo(ErrorStrategy.Action.THROW);
            }
        }

        @Test
        void connect_with_error_strategy_continue_with_max_attempts() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("error.strategy", "continue-with-max-attempts");
            settings.put("error.strategy.max-attempts", "4");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
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
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("error.strategy", "continue-with-max-attempts");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
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
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("error.strategy", "continue-with-time-limit");
            settings.put("error.strategy.time-limit-count-in-millis", "5000");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
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
    class Connect_with_high_execution_time {
        @Test
        @SuppressWarnings("java:s2925")
        void connect_with_error_strategy_continue_with_time_limit_without_time_limit() throws InterruptedException {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("error.strategy", "continue-with-time-limit");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
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
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            try (BackgroundEventSource ignored = sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
                sseConfiguration.start();
                assertThat(sseConfiguration.isStarted()).isTrue();
            }
        }

        @Test
        void not_connected() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            Assertions.assertThrows(IllegalStateException.class, sseConfiguration::start);
        }
    }

    @Nested
    class Stop {
        @Test
        void nominal_case() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            try (BackgroundEventSource ignored = sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
                sseConfiguration.start();
                sseConfiguration.stop();
                assertThat(sseConfiguration.isConnected()).isFalse();
                assertThat(sseConfiguration.isStarted()).isFalse();
            }
        }

        @Test
        void stop_without_previous_start_nor_connect() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            settings.put("configuration.id", "default");

            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.stop();
            assertThat(sseConfiguration.isConnected()).isFalse();
            assertThat(sseConfiguration.isStarted()).isFalse();
        }

        @Test
        void stop_without_previous_start() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            try (BackgroundEventSource ignored = sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
                sseConfiguration.stop();
                assertThat(sseConfiguration.isConnected()).isFalse();
                assertThat(sseConfiguration.isStarted()).isFalse();
            }
        }

        @Test
        void stop_without_previous_connect() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            OkHttpClient okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
            sseConfiguration.stop();
            assertThat(sseConfiguration.isConnected()).isFalse();
            assertThat(sseConfiguration.isStarted()).isFalse();
        }
    }

}