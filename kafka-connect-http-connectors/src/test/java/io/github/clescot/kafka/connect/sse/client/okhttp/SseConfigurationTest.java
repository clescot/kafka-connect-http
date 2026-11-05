package io.github.clescot.kafka.connect.sse.client.okhttp;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.ErrorStrategy;
import com.launchdarkly.eventsource.StreamHttpErrorException;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.client.http.okhttp.OkHttpClient;
import io.github.clescot.client.http.okhttp.OkHttpClientFactory;
import io.github.clescot.client.queue.QueueFactory;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.sse.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SseConfigurationTest {

  private static Map<String, String> baseSettings() {
    Map<String, String> settings = Maps.newHashMap();
    settings.put("url", "http://example.com/sse");
    settings.put("topic", "test-topic");
    return settings;
  }

  private static OkHttpClient buildClient(Map<String, String> settings) {
    return new OkHttpClientFactory()
        .buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
  }

  @Nested
  class Constructor {

    @Test
    void nominal_case() {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      assertThat(sseConfiguration.getConfigurationId()).isEqualTo("test-id");
      assertThat(sseConfiguration.getUri().toString()).hasToString("http://example.com/sse");
      assertThat(sseConfiguration.getTopic()).isEqualTo("test-topic");
      // error strategy, retry delay strategy and connect strategy should be null by default at
      // constructor
      // they are built when connect is called
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
      Assertions.assertThrows(
          NullPointerException.class, () -> new SseConfiguration(null, null, null));
    }

    @Test
    void null_args_with_configuration_id_set() {
      Assertions.assertThrows(
          NullPointerException.class, () -> new SseConfiguration("test", null, null));
    }

    @Test
    void null_args_with_configuration_id__and_http_client_configuration_set() {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      Assertions.assertThrows(
          NullPointerException.class, () -> new SseConfiguration("test", okHttpClient, null));
    }

    @Test
    void empty_settings() {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      HashMap<String, String> emptySettings = Maps.newHashMap();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> new SseConfiguration("test", okHttpClient, emptySettings));
    }

    @Test
    void resume_policy_defaults_to_from_committed_offset() {
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");

      SseConfiguration sseConfiguration =
          new SseConfiguration("test-id", buildClient(settings), settings);

      assertThat(sseConfiguration.getResumePolicy())
          .isEqualTo(SseResumePolicy.FROM_COMMITTED_OFFSET);
    }

    @Test
    void from_start_policy_with_a_static_last_event_id_header_is_refused() {
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      settings.put("enrich.request.static.header.names", "Last-Event-ID");
      settings.put("enrich.request.static.header.Last-Event-ID", "5");
      settings.put(SseConfigDefinition.RESUME_POLICY, SseConfigDefinition.RESUME_POLICY_FROM_START);

      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> new SseConfiguration("conflicting-id", buildClient(settings), settings));

      assertThat(exception).hasMessageContaining("conflicting-id");
    }

    @Test
    void a_static_last_event_id_header_on_its_own_is_accepted() {
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      settings.put("enrich.request.static.header.names", "Last-Event-ID");
      settings.put("enrich.request.static.header.Last-Event-ID", "5");

      Assertions.assertDoesNotThrow(
          () -> new SseConfiguration("static-id", buildClient(settings), settings));
    }

    @Test
    void an_unknown_resume_policy_is_refused() {
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      settings.put(SseConfigDefinition.RESUME_POLICY, "from-strat");

      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> new SseConfiguration("typo-id", buildClient(settings), settings));

      assertThat(exception).hasMessageContaining("from-strat");
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
      SseConfiguration sseConfiguration =
          SseConfiguration.buildSseConfiguration(
              "test-id", settings, null, new CompositeMeterRegistry(), okHttpClientFactory);
      assertThat(sseConfiguration.getConfigurationId()).isEqualTo("test-id");
      assertThat(sseConfiguration.getUri().toString()).hasToString("http://example.com/sse");
      assertThat(sseConfiguration.getTopic()).isEqualTo("test-topic");
    }

    @Test
    void null_settings() {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Assertions.assertThrows(
          NullPointerException.class,
          () ->
              SseConfiguration.buildSseConfiguration(
                  "test-id", null, null, new CompositeMeterRegistry(), okHttpClientFactory));
    }

    @Test
    void empty_settings() {
      HashMap<String, String> emptySettings = Maps.newHashMap();
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SseConfiguration.buildSseConfiguration(
                  "test-id",
                  emptySettings,
                  null,
                  new CompositeMeterRegistry(),
                  okHttpClientFactory));
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      settings.put("error.strategy", "dummy");
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      try (BackgroundEventSource ignored =
          sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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

    @Test
    void connect_with_committed_event_id_seeds_the_resume_cursor() {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      sseConfiguration.connect(
          QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME), Optional.of("7"));
      assertThat(sseConfiguration.getBackgroundEventSource().getEventSource().getLastEventId())
          .isEqualTo("7");
    }

    @Test
    void connect_without_committed_event_id_sends_no_resume_cursor() {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      sseConfiguration.connect(
          QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME), Optional.empty());
      assertThat(sseConfiguration.getBackgroundEventSource().getEventSource().getLastEventId())
          .isNullOrEmpty();
    }

    @Test
    void connect_with_null_committed_event_id() {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      Queue<SseEvent> queue = QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME);
      Assertions.assertThrows(
          NullPointerException.class, () -> sseConfiguration.connect(queue, null));
    }

    @Test
    void from_start_policy_adds_the_sentinel_when_no_offset_is_committed() {
      Map<String, String> settings = baseSettings();
      settings.put(SseConfigDefinition.RESUME_POLICY, SseConfigDefinition.RESUME_POLICY_FROM_START);
      SseConfiguration sseConfiguration =
          new SseConfiguration("test-id", buildClient(settings), settings);
      sseConfiguration.connect(
          QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME), Optional.empty());

      HttpRequest request = new HttpRequest("http://example.com/sse");
      sseConfiguration.applyResumePolicy(request);

      assertThat(request.getHeaders()).containsEntry("Last-Event-ID", List.of("0"));
    }

    @Test
    void from_start_policy_sends_an_empty_header_when_the_sentinel_is_empty() {
      Map<String, String> settings = baseSettings();
      settings.put(SseConfigDefinition.RESUME_POLICY, SseConfigDefinition.RESUME_POLICY_FROM_START);
      settings.put(SseConfigDefinition.RESUME_FROM_START_EVENT_ID, "");
      SseConfiguration sseConfiguration =
          new SseConfiguration("test-id", buildClient(settings), settings);
      sseConfiguration.connect(
          QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME), Optional.empty());

      HttpRequest request = new HttpRequest("http://example.com/sse");
      sseConfiguration.applyResumePolicy(request);

      assertThat(request.getHeaders()).containsKey("Last-Event-ID");
      assertThat(request.getHeaders().get("Last-Event-ID")).containsExactly("");
    }

    @Test
    void from_start_policy_leaves_the_library_cursor_alone() {
      Map<String, String> settings = baseSettings();
      settings.put(SseConfigDefinition.RESUME_POLICY, SseConfigDefinition.RESUME_POLICY_FROM_START);
      SseConfiguration sseConfiguration =
          new SseConfiguration("test-id", buildClient(settings), settings);
      sseConfiguration.connect(
          QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME), Optional.empty());
      HttpRequest request = new HttpRequest("http://example.com/sse");
      // the eventsource library lower-cases the header name it derives from its own cursor
      request.getHeaders().put("last-event-id", List.of("3"));

      sseConfiguration.applyResumePolicy(request);

      assertThat(request.getHeaders()).doesNotContainKey("Last-Event-ID");
      assertThat(request.getHeaders().get("last-event-id")).containsExactly("3");
    }

    @Test
    void from_start_policy_does_not_apply_when_an_offset_is_committed() {
      Map<String, String> settings = baseSettings();
      settings.put(SseConfigDefinition.RESUME_POLICY, SseConfigDefinition.RESUME_POLICY_FROM_START);
      SseConfiguration sseConfiguration =
          new SseConfiguration("test-id", buildClient(settings), settings);
      sseConfiguration.connect(
          QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME), Optional.of("7"));

      HttpRequest request = new HttpRequest("http://example.com/sse");
      sseConfiguration.applyResumePolicy(request);

      assertThat(request.getHeaders()).doesNotContainKey("Last-Event-ID");
    }

    @Test
    void the_default_policy_does_not_touch_the_request() {
      Map<String, String> settings = baseSettings();
      SseConfiguration sseConfiguration =
          new SseConfiguration("test-id", buildClient(settings), settings);
      sseConfiguration.connect(
          QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME), Optional.empty());

      HttpRequest request = new HttpRequest("http://example.com/sse");
      sseConfiguration.applyResumePolicy(request);

      assertThat(request.getHeaders()).doesNotContainKey("Last-Event-ID");
    }
  }

  @Nested
  class Connect_with_high_execution_time {
    @Test
    @SuppressWarnings("java:s2925")
    void connect_with_error_strategy_continue_with_time_limit_without_time_limit()
        throws InterruptedException {
      OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
      Map<String, String> settings = Maps.newHashMap();
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      try (BackgroundEventSource ignored =
          sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      try (BackgroundEventSource ignored =
          sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
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

      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      try (BackgroundEventSource ignored =
          sseConfiguration.connect(QueueFactory.getQueue(QueueFactory.DEFAULT_QUEUE_NAME))) {
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
      OkHttpClient okHttpClient =
          okHttpClientFactory.buildHttpClient(
              settings, null, new CompositeMeterRegistry(), new Random());
      settings.put("url", "http://example.com/sse");
      settings.put("topic", "test-topic");
      SseConfiguration sseConfiguration = new SseConfiguration("test-id", okHttpClient, settings);
      sseConfiguration.stop();
      assertThat(sseConfiguration.isConnected()).isFalse();
      assertThat(sseConfiguration.isStarted()).isFalse();
    }
  }
}
