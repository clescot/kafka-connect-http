package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.Task;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import static io.github.clescot.kafka.connect.sse.client.okhttp.SseConfiguration.buildSseConfiguration;

public class SseTask implements Task<OkHttpClient,SseConfiguration,HttpRequest, SseEvent> {

    private SseConnectorConfig sseConnectorConfig;
    private SseConfiguration sseConfiguration;
    private Queue<SseEvent> queue;
    private BackgroundEventSource backgroundEventSource;

    public SseTask(Map<String,String> settings) {

        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        this.sseConnectorConfig = new SseConnectorConfig(settings);
        Map<String,Object> mySettings = Maps.newHashMap(settings);
        this.sseConfiguration = buildSseConfiguration(mySettings);
        this.queue = QueueFactory.getQueue(String.valueOf(UUID.randomUUID()));
        this.backgroundEventSource = this.sseConfiguration.connect(queue,mySettings);
        this.backgroundEventSource.start();
    }


    @Override
    public Map<String, SseConfiguration> getConfigurations() {
        return Map.of();
    }

    public Queue<SseEvent> getQueue() {
        return queue;
    }


    public boolean isConnected() {
        return this.sseConfiguration.isConnected();
    }

    public void shutdown() {
        this.sseConfiguration.shutdown();
    }

    public String getTopic() {
        return this.sseConnectorConfig.getTopic();
    }

}
