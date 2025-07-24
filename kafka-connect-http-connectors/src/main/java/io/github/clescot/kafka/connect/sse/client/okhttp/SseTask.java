package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.Task;
import io.github.clescot.kafka.connect.http.client.HttpClientConfigurationFactory;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.CONFIGURATION_IDS;

public class SseTask implements Task<OkHttpClient,SseConfiguration,HttpRequest, SseEvent> {

    private SseConnectorConfig sseConnectorConfig;
    private Map<String,SseConfiguration> sseConfigurations;

    public SseTask(Map<String,String> settings) {

        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        this.sseConnectorConfig = new SseConnectorConfig(settings);
        Map<String,Object> mySettings = Maps.newHashMap(settings);
        this.sseConfigurations = HttpClientConfigurationFactory.buildConfigurations(
                new OkHttpClientFactory(),
                null,
                sseConnectorConfig.getList(CONFIGURATION_IDS),
                sseConnectorConfig.originals(),
                new CompositeMeterRegistry()
        ).entrySet().stream()
                .map(config -> Maps.immutableEntry(
                        config.getKey(),
                        new SseConfiguration(config.getValue())
                )).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
        this.sseConfigurations.forEach((name, config) -> {
            config.connect(QueueFactory.getQueue(name),mySettings);
            config.start();
        });
    }


    @Override
    public Map<String, SseConfiguration> getConfigurations() {
        return this.sseConfigurations;
    }
    public Collection<Queue<SseEvent>> getQueues() {
        return this.sseConfigurations.values().stream()
                .map(SseConfiguration::getQueue)
                .toList();
    }
    public Queue<SseEvent> getQueue(String configurationId) {
        if( this.sseConfigurations.containsKey(configurationId)) {
            return this.sseConfigurations.get(configurationId).getQueue();
        }
        return null;
    }


    public boolean isConnected(String configurationId) {
        if( this.sseConfigurations.containsKey(configurationId)) {
            return this.sseConfigurations.get(configurationId).isConnected();
        }
        return false;
    }

    public void shutdown() {
        this.sseConfigurations.forEach((name,configuration)-> configuration.shutdown());
    }

    public String getDefaultTopic() {
        return this.sseConnectorConfig.getDefaultTopic();
    }



}
