package io.github.clescot.kafka.connect.sse.client.okhttp;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Maps;
import io.github.clescot.core.sse.SseEvent;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Round trip of the advertised-delay pacing through launchdarkly-eventsource: the first connection
 * attempt is refused with an exhausted {@code RateLimit} window and a shorter {@code Retry-After},
 * the second is accepted and streams events. Proves that the pacing reaches the library's own
 * reconnect delay, that {@code Retry-After} wins, and that the stream still delivers afterwards.
 */
class ITSseRateLimitPacingTest {

  private static final String STREAM_PATH = "/stream";
  private static final String STREAMING = "streaming";

  @RegisterExtension
  static WireMockExtension wireMock =
      WireMockExtension.newInstance()
          .options(
              WireMockConfiguration.wireMockConfig()
                  .dynamicPort()
                  .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER))
          .build();

  private SseSourceTask sseSourceTask;

  @BeforeEach
  void setup() {
    sseSourceTask = new SseSourceTask();
  }

  @AfterEach
  void tearDown() {
    if (sseSourceTask != null) {
      sseSourceTask.stop();
    }
  }

  @Test
  void the_next_connection_attempt_waits_for_the_advertised_retry_after() {
    wireMock.stubFor(
        get(urlEqualTo(STREAM_PATH))
            .inScenario("pace")
            .whenScenarioStateIs(STARTED)
            .willReturn(
                aResponse()
                    .withStatus(429)
                    .withHeader("RateLimit", "\"default\";r=0;t=30")
                    .withHeader("Retry-After", "2")
                    .withBody("slow down"))
            .willSetStateTo(STREAMING));
    wireMock.stubFor(
        get(urlEqualTo(STREAM_PATH))
            .inScenario("pace")
            .whenScenarioStateIs(STREAMING)
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/event-stream")
                    .withBody(
                        "id: 1\nevent: one\ndata: first\n\n"
                            + "id: 2\nevent: two\ndata: second\n\n")));

    sseSourceTask.start(settings());

    long firstAttempt = awaitAttempt(1);
    long secondAttempt = awaitAttempt(2);

    // Retry-After (2 s) wins over the 30 s RateLimit window; the library backoff alone is 1 s
    assertThat(secondAttempt - firstAttempt).isGreaterThanOrEqualTo(1900L);

    Queue<SseEvent> queue = sseSourceTask.getQueue("default").orElseThrow();
    Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> queue.size() >= 2);
    assertThat(sseSourceTask.isConnected("default")).isTrue();
  }

  private Map<String, String> settings() {
    WireMockRuntimeInfo runtimeInfo = wireMock.getRuntimeInfo();
    Map<String, String> settings = Maps.newHashMap();
    settings.put("config.ids", "default");
    settings.put("config.default.url", runtimeInfo.getHttpBaseUrl() + STREAM_PATH);
    settings.put("config.default.topic", "dummy_topic");
    return settings;
  }

  private long awaitAttempt(int attemptNumber) {
    long deadline = System.currentTimeMillis() + 20_000L;
    while (System.currentTimeMillis() < deadline) {
      if (wireMock.findAll(getRequestedFor(urlEqualTo(STREAM_PATH))).size() >= attemptNumber) {
        return System.currentTimeMillis();
      }
      try {
        Thread.sleep(20L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new AssertionError("connection attempt " + attemptNumber + " never reached the server");
  }
}
