package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.ReadyState;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ITSseSourceTaskTest {

    @Container
    public GenericContainer sseServerContainer = new GenericContainer(DockerImageName.parse("jmalloc/echo-server:v0.3.7"))
            .withExposedPorts(8080)
            .withNetworkAliases("sse-server");

    SseSourceTask sseSourceTask;

    @BeforeEach
    void setup() {
        sseSourceTask = new SseSourceTask();
    }


    @Test
    void test_settings_with_local_sse() {
        Integer mappedPort = sseServerContainer.getMappedPort(8080);
        Map<String, String> settings = Maps.newHashMap();
        settings.put("topic", "test");
        String configurationId = "test_sse_client_connect";
        settings.put("configuration.id", configurationId);
        settings.put("url", "http://localhost:"+mappedPort+"/.sse");
        settings.put("okhttp.retry.on.connection.failure", "true");
        Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
        Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId);
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(()->!queue.isEmpty());
        queue.stream().forEach(msg-> System.out.println("############### :"+msg));
        Awaitility.await().atMost(40, TimeUnit.SECONDS).until(()->queue.size()>20);
        assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
        sseSourceTask.stop();
        assertThat(sseSourceTask.isConnected(configurationId)).isFalse();
        EventSource eventSource = sseSourceTask.getBackgroundEventSource().getEventSource();
        assertThat(eventSource.getState()).isEqualTo(ReadyState.CLOSED);
        assertThat(eventSource.getLastEventId()).isEqualTo("21");
    }

}
