package io.github.clescot.kafka.connect.sse.client.okhttp;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okForContentType;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.client.Constants.CONFIGURATION_IDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Maps;
import io.github.clescot.client.queue.QueueFactory;
import io.github.clescot.core.sse.SseEvent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
class SseSourceTaskTest {

  @RegisterExtension static WireMockExtension wmHttp;

  static {
    wmHttp =
        WireMockExtension.newInstance()
            .options(
                WireMockConfiguration.wireMockConfig()
                    .dynamicPort()
                    .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())
                    .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER))
            .build();
  }

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
      Assertions.assertThrows(IllegalArgumentException.class, () -> sseSourceTask.start(settings));
    }

    @Test
    void test_null_settings() {
      Assertions.assertThrows(NullPointerException.class, () -> sseSourceTask.start(null));
    }

    @Test
    void test_settings_with_topic_only() {
      Map<String, String> settings = Maps.newHashMap();
      settings.put("config.ids", "default");
      settings.put("config.default.topic", "test");
      Assertions.assertThrows(NullPointerException.class, () -> sseSourceTask.start(settings));
    }

    @Test
    void test_settings_with_topic_and_url() {
      Map<String, String> settings = Maps.newHashMap();
      String configurationId = "gggggg888";
      settings.put("config.ids", configurationId);
      settings.put("config." + configurationId + ".url", "http://localhost:8080/sse");
      settings.put("config." + configurationId + ".topic", "dummy_topic");
      Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
    }

    @Test
    void test_settings_with_multiple_configurations() {
      Map<String, String> settings = Maps.newHashMap();
      settings.put(CONFIGURATION_IDS, "test");
      settings.put("config.default.url", "http://localhost:8080/sse");
      settings.put("config.default.topic", "dummy_topic");
      settings.put("config.test.url", "http://localhost:8080/sse2");
      settings.put("config.test.topic", "dummy_topic2");
      Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
    }

    @Test
    void test_settings_with_multiple_configurations_and_default_defined() {
      Map<String, String> settings = Maps.newHashMap();
      settings.put(CONFIGURATION_IDS, "default,test");
      settings.put("config.default.url", "http://localhost:8080/sse");
      settings.put("config.default.topic", "dummy_topic");
      settings.put("config.test.url", "http://localhost:8080/sse2");
      settings.put("config.test.topic", "dummy_topic2");
      Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
    }

    @Test
    void test_settings_with_static_request_header() {
      Map<String, String> settings = Maps.newHashMap();
      String configurationId = "dfddfddd44444";
      settings.put("config.ids", configurationId);
      settings.put("config." + configurationId + ".url", "http://localhost:8080/sse");
      settings.put("config." + configurationId + ".topic", "dummy_topic");
      settings.put("config." + configurationId + ".enrich.request.static.header.names", "auth1");
      settings.put("config." + configurationId + ".enrich.request.static.header.auth1", "value1");
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
      SseSourceTask sseSourceTask = new SseSourceTask();
      String version = sseSourceTask.version();
      assertThat(version).isNotNull().isNotBlank();
    }
  }

  @Nested
  class Poll {
    SseSourceTask sseSourceTask;
    WireMockRuntimeInfo wmRuntimeInfo;

    @BeforeEach
    void setup() {
      QueueFactory.clearRegistrations();
      QueueFactory.clearQueueMap();
      sseSourceTask = new SseSourceTask();
      var url1 = "/events1";
      var url2 = "/events2";

      // prepare the WireMock server to simulate an SSE endpoint
      String scenario = "test_sse_client_connect";
      wmRuntimeInfo = wmHttp.getRuntimeInfo();
      WireMock wireMock = wmRuntimeInfo.getWireMock();
      String dataStream1 =
          """
                id:1
                event:random
                data:{"id":"test post 1", "createdAt":"2025-04-26T10:15:30"}

                id:2
                event:type2
                data:{"id":"test post 2", "createdAt":"2025-04-26T10:15:31"}

                """;
      String dataStream2 =
          """
                id:1
                event:type3
                data:{"id":"test post 1", "createdAt":"2025-04-26T10:15:30"}

                id:2
                event:type4
                data:{"id":"test post 2", "createdAt":"2025-04-26T10:15:31"}

                id:3
                event:type4
                data:{"id":"test post 3", "createdAt":"2025-04-26T10:15:31"}

                id:4
                event:type4
                data:{"id":"test post 4", "createdAt":"2025-04-26T10:15:31"}

                """;
      wireMock.register(
          WireMock.get(url1)
              .inScenario(scenario)
              .whenScenarioStateIs(STARTED)
              .willReturn(okForContentType("text/event-stream", dataStream1))
              .willSetStateTo("CONNECTED"));
      wireMock.register(
          WireMock.get(url2)
              .withHeader("auth1", equalTo("value1"))
              .inScenario(scenario)
              .whenScenarioStateIs(STARTED)
              .willReturn(okForContentType("text/event-stream", dataStream2))
              .willSetStateTo("CONNECTED"));
    }

    @AfterEach
    void shutdown() {
      sseSourceTask.stop();
    }

    @Test
    void test_polling_nominal_case() {
      Map<String, String> settings = Maps.newHashMap();
      String configurationId = "poll1234";
      settings.put("config.ids", configurationId);
      settings.put("config." + configurationId + ".topic", "test");
      settings.put(
          "config." + configurationId + ".url", wmRuntimeInfo.getHttpBaseUrl() + "/events1");
      sseSourceTask.start(settings);
      assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
      Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId).orElseThrow();
      Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> !queue.isEmpty());
      assertThat(queue).hasSize(2);
      Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> !sseSourceTask.poll().isEmpty());
    }

    @Test
    void test_polling_with_static_header() {
      Map<String, String> settings = Maps.newHashMap();
      String configurationId = "df646sd";
      settings.put("config.ids", configurationId);
      settings.put("config." + configurationId + ".topic", "test");
      settings.put(
          "config." + configurationId + ".url", wmRuntimeInfo.getHttpBaseUrl() + "/events2");
      settings.put("config." + configurationId + ".enrich.request.static.header.names", "auth1");
      settings.put("config." + configurationId + ".enrich.request.static.header.auth1", "value1");
      sseSourceTask.start(settings);
      assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
      Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId).orElseThrow();
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !queue.isEmpty());
      assertThat(queue).hasSize(4);
      Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !sseSourceTask.poll().isEmpty());
    }

    @Test
    void test_polling_with_connect_timeout_set() {
      Map<String, String> settings = Maps.newHashMap();
      String queueName = "default1";
      settings.put("config.ids", queueName);
      settings.put("config." + queueName + ".topic", "test");
      settings.put("config." + queueName + ".url", wmRuntimeInfo.getHttpBaseUrl() + "/events1");
      settings.put("config." + queueName + ".okhttp.connect.timeout", "3000");
      sseSourceTask.start(settings);
      assertThat(sseSourceTask.isConnected(queueName)).isTrue();
      Queue<SseEvent> queue = sseSourceTask.getQueue(queueName).orElseThrow();
      Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> !queue.isEmpty());
      assertThat(queue).hasSize(2);
      Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> !sseSourceTask.poll().isEmpty());
    }

    @Test
    void test_polling_keys_source_records_by_configuration_id_not_by_event_type() {
      Map<String, String> settings = Maps.newHashMap();
      String configurationId = "poll1234";
      settings.put("config.ids", configurationId);
      settings.put("config." + configurationId + ".topic", "test");
      settings.put(
          "config." + configurationId + ".url", wmRuntimeInfo.getHttpBaseUrl() + "/events1");
      sseSourceTask.start(settings);
      assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
      Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId).orElseThrow();
      Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> !queue.isEmpty());

      List<SourceRecord> records = sseSourceTask.poll();

      assertThat(records).isNotEmpty();
      // the partition is the SSE connection, so a restart can resume from the committed offset
      for (SourceRecord sourceRecord : records) {
        assertThat(sourceRecord.sourcePartition())
            .isEqualTo(Map.of("configurationId", configurationId));
      }
      assertThat(records.get(0).sourceOffset()).isEqualTo(Map.of("eventId", "1"));
    }
  }

  @Nested
  class Resume {
    SseSourceTask sseSourceTask;
    WireMockRuntimeInfo wmRuntimeInfo;

    /** Plays the role of the Kafka Connect runtime, which sets the protected context field. */
    private SseSourceTask sseSourceTaskWithCommittedOffset(
        String configurationId, String committedEventId) {
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      Map<String, String> partition = Map.of("configurationId", configurationId);
      Map<String, Object> committedOffset = Map.of("eventId", committedEventId);
      when(offsetStorageReader.offset(partition)).thenReturn(committedOffset);
      SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
      when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
      return new SseSourceTask() {
        {
          context = sourceTaskContext;
        }
      };
    }

    @BeforeEach
    void setup() {
      QueueFactory.clearRegistrations();
      QueueFactory.clearQueueMap();
      wmRuntimeInfo = wmHttp.getRuntimeInfo();
    }

    @AfterEach
    void shutdown() {
      if (sseSourceTask != null) {
        sseSourceTask.stop();
      }
    }

    private void registerQuietEventStream(String url) {
      // an empty stream keeps the connection open without delivering an event, so the seeded
      // resume cursor is not overwritten by a received event id before the assertion runs
      wmRuntimeInfo
          .getWireMock()
          .register(WireMock.get(url).willReturn(okForContentType("text/event-stream", "")));
    }

    private Map<String, String> settingsFor(String configurationId) {
      return settingsFor(configurationId, "/events1");
    }

    private Map<String, String> settingsFor(String configurationId, String path) {
      Map<String, String> settings = Maps.newHashMap();
      settings.put("config.ids", configurationId);
      settings.put("config." + configurationId + ".topic", "test");
      settings.put("config." + configurationId + ".url", wmRuntimeInfo.getHttpBaseUrl() + path);
      return settings;
    }

    private Map<String, String> settingsFor(
        String configurationId, String path, String resumePolicy) {
      Map<String, String> settings = settingsFor(configurationId, path);
      settings.put(
          "config." + configurationId + "." + SseConfigDefinition.RESUME_POLICY, resumePolicy);
      return settings;
    }

    /** Plays the role of the Kafka Connect runtime of a task that has no committed offset yet. */
    private SseSourceTask sseSourceTaskWithEmptyOffsetStorage() {
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(null);
      SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
      when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
      return new SseSourceTask() {
        {
          context = sourceTaskContext;
        }
      };
    }

    @Test
    void test_resume_from_the_committed_offset() {
      String configurationId = "resume123";
      registerQuietEventStream("/events1");
      sseSourceTask = sseSourceTaskWithCommittedOffset(configurationId, "7");

      sseSourceTask.start(settingsFor(configurationId));

      assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
      assertThat(
              sseSourceTask
                  .getConfigurations()
                  .get(configurationId)
                  .getBackgroundEventSource()
                  .getEventSource()
                  .getLastEventId())
          .isEqualTo("7");
    }

    @Test
    void test_no_committed_offset_lets_the_server_resume_policy_apply() {
      String configurationId = "resume456";
      registerQuietEventStream("/events1");
      sseSourceTask = new SseSourceTask();

      sseSourceTask.start(settingsFor(configurationId));

      assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
      assertThat(
              sseSourceTask
                  .getConfigurations()
                  .get(configurationId)
                  .getBackgroundEventSource()
                  .getEventSource()
                  .getLastEventId())
          .isNullOrEmpty();
    }

    @Test
    void test_from_start_policy_asks_for_the_whole_stream_on_a_cold_start() {
      String configurationId = "fromstart123";
      registerQuietEventStream("/from-start");
      sseSourceTask = new SseSourceTask();

      sseSourceTask.start(
          settingsFor(
              configurationId, "/from-start", SseConfigDefinition.RESUME_POLICY_FROM_START));

      assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  wmRuntimeInfo
                      .getWireMock()
                      .verifyThat(
                          getRequestedFor(urlEqualTo("/from-start"))
                              .withHeader("Last-Event-ID", equalTo("0"))));
      // the sentinel is not the library's cursor: the seed stays empty
      assertThat(
              sseSourceTask
                  .getConfigurations()
                  .get(configurationId)
                  .getBackgroundEventSource()
                  .getEventSource()
                  .getLastEventId())
          .isNullOrEmpty();
    }

    @Test
    void test_from_start_policy_still_resumes_from_a_committed_offset() {
      String configurationId = "fromstart456";
      String path = "/from-start-committed";
      registerQuietEventStream(path);
      sseSourceTask = sseSourceTaskWithCommittedOffset(configurationId, "7");

      sseSourceTask.start(
          settingsFor(configurationId, path, SseConfigDefinition.RESUME_POLICY_FROM_START));

      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  wmRuntimeInfo
                      .getWireMock()
                      .verifyThat(
                          getRequestedFor(urlEqualTo(path))
                              .withHeader("Last-Event-ID", equalTo("7"))));
      // the policy does not apply where an offset is committed: no sentinel is ever asked for
      wmRuntimeInfo
          .getWireMock()
          .verifyThat(
              0, getRequestedFor(urlEqualTo(path)).withHeader("Last-Event-ID", equalTo("0")));
    }

    @Test
    void test_fail_policy_without_a_committed_offset_fails_the_task() {
      String configurationId = "fail123";
      registerQuietEventStream("/fail");
      sseSourceTask = sseSourceTaskWithEmptyOffsetStorage();

      IllegalStateException exception =
          Assertions.assertThrows(
              IllegalStateException.class,
              () ->
                  sseSourceTask.start(
                      settingsFor(
                          configurationId, "/fail", SseConfigDefinition.RESUME_POLICY_FAIL)));

      assertThat(exception).hasMessageContaining(configurationId);
    }

    @Test
    void test_fail_policy_without_offset_storage_does_not_fail_the_task() {
      String configurationId = "fail456";
      registerQuietEventStream("/fail-no-storage");
      // no context: the task was never started by Connect, so "nothing committed" says nothing
      // about the configuration
      sseSourceTask = new SseSourceTask();

      sseSourceTask.start(
          settingsFor(configurationId, "/fail-no-storage", SseConfigDefinition.RESUME_POLICY_FAIL));

      assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
    }
  }

  @Nested
  class ResumeLookup {

    @Test
    void a_task_never_started_by_connect_has_no_offset_storage() {
      SseSourceTask sseSourceTask = new SseSourceTask();

      assertThat(sseSourceTask.resumeLookup().hasOffsetStorage()).isFalse();
      assertThat(sseSourceTask.resumeLookup().lastCommittedEventId("any")).isEmpty();
    }

    @Test
    void a_task_started_by_connect_has_offset_storage() {
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
      when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
      SseSourceTask sseSourceTask =
          new SseSourceTask() {
            {
              context = sourceTaskContext;
            }
          };

      assertThat(sseSourceTask.resumeLookup().hasOffsetStorage()).isTrue();
      assertThat(sseSourceTask.resumeLookup().lastCommittedEventId("any")).isEmpty();
    }
  }
}
