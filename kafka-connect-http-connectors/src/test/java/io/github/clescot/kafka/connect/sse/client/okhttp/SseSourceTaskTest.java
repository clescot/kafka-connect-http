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

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
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

        @Test
        void test_settings_with_static_request_header() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.url", "http://localhost:8080/sse");
            settings.put("config.default.topic", "dummy_topic");
            settings.put("config.default.enrich.request.static.header.names", "auth1");
            settings.put("config.default.enrich.request.static.header.auth1", "value1");
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
            assertThat(version)
                    .isNotNull()
                    .isNotBlank();
        }
    }

    @Nested
    class Poll{
        SseSourceTask sseSourceTask;
        WireMockRuntimeInfo wmRuntimeInfo;
        @BeforeEach
        void setup() {
            sseSourceTask = new SseSourceTask();
            var url1 = "/events1";
            var url2 = "/events2";

            //prepare the WireMock server to simulate an SSE endpoint
            String scenario = "test_sse_client_connect";
            wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String dataStream1 = """
                id:1
                event:random
                data:{"id":"test post 1", "createdAt":"2025-04-26T10:15:30"}
                
                id:2
                event:type2
                data:{"id":"test post 2", "createdAt":"2025-04-26T10:15:31"}
                
                """;
            String dataStream2 = """
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
            wireMock
                    .register(WireMock.get(url1).inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(okForContentType("text/event-stream", dataStream1))
                            .willSetStateTo("CONNECTED")
                    );
            wireMock
                    .register(WireMock
                            .get(url2)
                            .withHeader("auth1", equalTo("value1"))
                            .inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(okForContentType("text/event-stream", dataStream2))
                            .willSetStateTo("CONNECTED")
                    );
        }

        @AfterEach
        void shutdown() {
            sseSourceTask.stop();
        }

        @Test
        void test_nominal_case() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.topic", "test");
            settings.put("config.default.url", wmRuntimeInfo.getHttpBaseUrl()+"/events1");
            sseSourceTask.start(settings);
            assertThat(sseSourceTask.isConnected("default")).isTrue();
            Queue<SseEvent> queue = sseSourceTask.getQueue("default").orElseThrow();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()-> !queue.isEmpty());
            assertThat(queue).hasSize(2);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()->!sseSourceTask.poll().isEmpty());

        }

        @Test
        void test_with_static_header() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.default.topic", "test");
            settings.put("config.default.url", wmRuntimeInfo.getHttpBaseUrl()+"/events2");
            settings.put("config.default.enrich.request.static.header.names", "auth1");
            settings.put("config.default.enrich.request.static.header.auth1", "value1");
            sseSourceTask.start(settings);
            assertThat(sseSourceTask.isConnected("default")).isTrue();
            Queue<SseEvent> queue = sseSourceTask.getQueue("default").orElseThrow();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()-> !queue.isEmpty());
            assertThat(queue).hasSize(4);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()->!sseSourceTask.poll().isEmpty());
        }
    }

}