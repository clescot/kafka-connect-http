package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.Task;
import io.github.clescot.kafka.connect.http.client.HttpClientConfigurationFactory;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.CONFIGURATION_IDS;

public class SseTask implements Task<OkHttpClient,SseConfiguration,HttpRequest, SseEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SseTask.class);
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
                        new SseConfiguration(config.getKey(), config.getValue(), MapUtils.getMapWithPrefix(mySettings,"config."+config.getKey()+"."))
                )).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
        this.sseConfigurations.forEach((name, config) -> {
            config.connect(QueueFactory.getQueue(name));
            config.start();
        });
    }


    @Override
    public Map<String, SseConfiguration> getConfigurations() {
        return this.sseConfigurations;
    }
    public Map<String,Queue<SseEvent>> getQueues() {
        return this.sseConfigurations.entrySet().stream()
                .map(entry-> Map.entry(entry.getKey(),entry.getValue().getQueue()))
                .collect(Collectors.toMap(entry->entry.getKey(), entry->entry.getValue()));
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
