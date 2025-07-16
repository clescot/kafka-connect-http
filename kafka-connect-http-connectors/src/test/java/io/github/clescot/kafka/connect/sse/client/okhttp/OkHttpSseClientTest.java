package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class OkHttpSseClientTest {

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
                data:{"id":"test post 1", "createdAt":"2023-03-26T10:15:30"}
                
                id:2
                event:type2
                data:{"id":"test post 2", "createdAt":"2023-03-26T10:15:31"}
                
                """;
        wireMock
                .register(WireMock.get(url).inScenario(scenario)
                        .whenScenarioStateIs(STARTED)
                        .willReturn(okForContentType("text/event-stream", dataStream))
                        .willSetStateTo("CONNECTED")
                );

        // Create an OkHttpSseClient instance and connect to the WireMock server
        OkHttpClientFactory factory = new OkHttpClientFactory();
        Map<String,Object> okHttpClientConfig = Maps.newHashMap();
        okHttpClientConfig.put("configuration.id", "test_sse_client_connect");
        OkHttpClient okHttpClient = factory.buildHttpClient(okHttpClientConfig, null, new CompositeMeterRegistry(), new Random());
        OkHttpSseClient client = new OkHttpSseClient(okHttpClient.getInternalClient(), QueueFactory.getQueue("test_sse_client_connect"));

        // Connect to the SSE endpoint
        Map<String, String> config = Maps.newHashMap();
        config.put("url", wmHttp.url("/events"));
        assertDoesNotThrow(() -> client.connect(config));
        assertTrue(client.isConnected());


        Stream<SseEvent> eventStream = client.getEventStream();
        assertNotNull(eventStream);

        List<SseEvent> events = eventStream.toList();
        assertThat(events).hasSize(2);
        SseEvent firstEvent = events.get(0);
        assertThat(firstEvent.getId()).isEqualTo("1");
        assertThat(firstEvent.getType()).isEqualTo("random");
        assertThat(firstEvent.getData()).isEqualTo("""
                {"id":"test post 1", "createdAt":"2023-03-26T10:15:30"}
                """);
        SseEvent secondEvent = events.get(1);
        assertThat(secondEvent.getId()).isEqualTo("2");
        assertThat(secondEvent.getType()).isEqualTo("type2");
        assertThat(secondEvent.getData()).isEqualTo("""
                {"id":"test post 2", "createdAt":"2023-03-26T10:15:31"}
                """);
    }
}