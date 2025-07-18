package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.quartz.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.okForContentType;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
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
            settings.put("topic", "test");
            Assertions.assertThrows(ConfigException.class, () -> sseSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_url() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("configuration.id", "test_sse_client_connect");
            settings.put("url", "http://localhost:8080/sse");
            Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
        }
//        @Test
//        void test_settings_with_sse_dev() {
//            Map<String, String> settings = Maps.newHashMap();
//            settings.put("topic", "test");
//            settings.put("configuration.id", "test_sse_client_connect");
//            settings.put("url", "https://sse.dev/test");
//            settings.put("okhttp.retry.on.connection.failure", "true");
//            Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
//            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(()->!sseSourceTask.getQueue().isEmpty());
//            sseSourceTask.getQueue().stream().forEach(msg-> System.out.println(msg));
//        }




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
            settings.put("configuration.id", "test_sse_client_connect");
            settings.put("topic", "test");
            settings.put("url", wmRuntimeInfo.getHttpBaseUrl()+"/events");
            SseSourceTask.start(settings);
            assertThat(SseSourceTask.isConnected()).isTrue();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()-> SseSourceTask.getQueue().peek()!=null);
            assertThat(SseSourceTask.getQueue()).hasSize(2);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()->!SseSourceTask.poll().isEmpty());

        }
    }

}