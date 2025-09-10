package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.github.tomakehurst.wiremock.client.WireMock.okForContentType;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.sse.client.okhttp.SseConfiguration.buildSseConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class SseHttpClientConfigurationTest {

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


    @Test
    void test_sse_client_connect() {
        var url = "/events";

        //prepare the WireMock server to simulate an SSE endpoint
        String scenario = "test_sse_client_connect";
        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
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

        // Create an OkHttpSseClient instance and connect to the WireMock server
        Map<String,String> settings = Maps.newHashMap();
        settings.put("url", wmHttp.url("/events"));
        settings.put("topic", "test-topic");
        ExecutorService service = java.util.concurrent.Executors.newSingleThreadExecutor();
        SseConfiguration client = buildSseConfiguration(Configuration.DEFAULT_CONFIGURATION_ID, settings,service,new CompositeMeterRegistry(),new OkHttpClientFactory());

        // Connect to the SSE endpoint

        assertDoesNotThrow(() -> {
            client.connect(QueueFactory.getQueue(String.valueOf(UUID.randomUUID())));
            client.start();
        });
        assertTrue(client.isConnected());
        Queue<SseEvent> eventQueue = client.getQueue();
        Awaitility.await().atMost(5, java.util.concurrent.TimeUnit.SECONDS)
                .until(() -> !eventQueue.isEmpty());

        assertNotNull(eventQueue);

        assertThat(eventQueue).hasSize(2);
        SseEvent firstEvent = eventQueue.poll();
        assertThat(firstEvent.getId()).isEqualTo("1");
        assertThat(firstEvent.getType()).isEqualTo("random");
        assertThat(firstEvent.getData()).isEqualTo("""
                {"id":"test post 1", "createdAt":"2025-04-26T10:15:30"}
                """.strip());
        SseEvent secondEvent = eventQueue.poll();
        assertThat(secondEvent.getId()).isEqualTo("2");
        assertThat(secondEvent.getType()).isEqualTo("type2");
        assertThat(secondEvent.getData()).isEqualTo("""
                {"id":"test post 2", "createdAt":"2025-04-26T10:15:31"}
                """.strip());
        client.stop();
        assertThat(client.isConnected()).isFalse();
        assertThat(eventQueue).isEmpty();
    }
}