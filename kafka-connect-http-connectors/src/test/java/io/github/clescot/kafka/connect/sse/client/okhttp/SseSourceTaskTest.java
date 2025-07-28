package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.apache.kafka.common.config.ConfigException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.okForContentType;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.CONFIGURATION_IDS;
import static org.assertj.core.api.Assertions.assertThat;

class SseSourceTaskTest {

    @RegisterExtension
    static WireMockExtension wmHttp;

    static {

        wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                            .dynamicPort()
                            .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())
                            .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                )
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
            Assertions.assertThrows(ConfigException.class, () -> sseSourceTask.start(settings));
        }

        @Test
        void test_null_settings() {
            Assertions.assertThrows(NullPointerException.class, () -> sseSourceTask.start(null));
        }

        @Test
        void test_settings_with_topic_only() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.topic", "test");
            Assertions.assertThrows(ConfigException.class, () -> sseSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_url() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.url", "http://localhost:8080/sse");
            settings.put("config.default.topic", "dummy_topic");
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
        WireMockRuntimeInfo wmRuntimeInfo;
        @BeforeEach
        void setup() {
            SseSourceTask = new SseSourceTask();
            var url = "/events";

            //prepare the WireMock server to simulate an SSE endpoint
            String scenario = "test_sse_client_connect";
            wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String dataStream = """
                id:1
                event:random
                data:{"id":"test post 1", "createdAt":"2025-04-26T10:15:30"}
                
                id:2
                event:type2
                data:{"id":"test post 2", "createdAt":"2025-04-26T10:15:31"}
                
                """;
            wireMock
                    .register(WireMock.get(url).inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(okForContentType("text/event-stream", dataStream))
                            .willSetStateTo("CONNECTED")
                    );
        }

        @AfterEach
        void shutdown() {
            SseSourceTask.stop();
        }

        @Test
        void test_nominal_case() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.topic", "test");
            settings.put("config.default.url", wmRuntimeInfo.getHttpBaseUrl()+"/events");
            SseSourceTask.start(settings);
            assertThat(SseSourceTask.isConnected("default")).isTrue();
            Queue<SseEvent> queue = SseSourceTask.getQueue("default").orElseThrow();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()-> !queue.isEmpty());
            assertThat(queue).hasSize(2);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()->!SseSourceTask.poll().isEmpty());

        }
    }

}