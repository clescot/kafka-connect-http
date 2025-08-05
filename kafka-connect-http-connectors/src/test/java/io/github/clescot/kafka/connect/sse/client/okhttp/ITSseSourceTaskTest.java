package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.ReadyState;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.CONFIGURATION_IDS;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ITSseSourceTaskTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ITSseSourceTaskTest.class);
    @Container
    public GenericContainer sseServerContainer = new GenericContainer(DockerImageName.parse("jmalloc/echo-server:v0.3.7"))
            .withExposedPorts(8080)
            .withNetworkAliases("sse-server");

    SseSourceTask sseSourceTask;

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
    void test_settings_with_local_sse() {
        Integer mappedPort = sseServerContainer.getMappedPort(8080);
        Map<String, String> settings = Maps.newHashMap();
        settings.put("topic", "test");
        String configurationId = "default";
        settings.put("config.ids", "default");
        settings.put("config.default.url", "http://localhost:"+mappedPort+"/.sse");
        settings.put("config.default.topic", "dummy_topic");
        settings.put("okhttp.retry.on.connection.failure", "true");
        Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
        Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId).orElseThrow();
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(()->!queue.isEmpty());
        Awaitility.await().atMost(40, TimeUnit.SECONDS).until(()->queue.size()>20);
        assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
        sseSourceTask.stop();
        assertThat(sseSourceTask.isConnected(configurationId)).isFalse();
        EventSource eventSource = sseSourceTask.getConfigurations().get(Configuration.DEFAULT_CONFIGURATION_ID).getBackgroundEventSource().getEventSource();
        assertThat(eventSource.getState()).isEqualTo(ReadyState.CLOSED);
        queue.forEach(msg-> LOGGER.info("############### :{}",msg));
        assertThat(eventSource.getLastEventId()).isEqualTo("21");
    }


    @Test
    void test_settings_with_multiple_local_sse() {
        Integer mappedPort = sseServerContainer.getMappedPort(8080);
        Map<String, String> settings = Maps.newHashMap();
        settings.put("topic", "test");
        String configurationId = "test_sse_client_connect";
        settings.put(CONFIGURATION_IDS, "default,"+configurationId);
        settings.put("config.default.url", "http://localhost:"+mappedPort+"/.sse");
        settings.put("config.default.topic", "dummy_topic");
        settings.put("config."+configurationId+".url", "http://localhost:"+mappedPort+"/.sse");
        settings.put("config."+configurationId+".topic","dummy_topic2");
        settings.put("okhttp.retry.on.connection.failure", "true");
        Assertions.assertDoesNotThrow(() -> sseSourceTask.start(settings));
        Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId).orElseThrow();
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(()->!queue.isEmpty());
        Awaitility.await().atMost(40, TimeUnit.SECONDS).until(()->queue.size()>20);
        assertThat(sseSourceTask.isConnected(configurationId)).isTrue();
        sseSourceTask.stop();
        assertThat(sseSourceTask.isConnected(configurationId)).isFalse();
        EventSource eventSource = sseSourceTask.getConfigurations().get(SseConfiguration.DEFAULT_CONFIGURATION_ID).getBackgroundEventSource().getEventSource();
        assertThat(eventSource.getState()).isEqualTo(ReadyState.CLOSED);
        assertThat(eventSource.getLastEventId()).isEqualTo("21");
        Set<Map.Entry<String, Queue<SseEvent>>> entries = sseSourceTask.getQueues().entrySet();
        for (Map.Entry<String, Queue<SseEvent>> entry : entries) {
            entry.getValue().forEach(msg-> LOGGER.info("############### configId :{} ############### event :{}",entry.getKey(),msg));
        }
    }



}
