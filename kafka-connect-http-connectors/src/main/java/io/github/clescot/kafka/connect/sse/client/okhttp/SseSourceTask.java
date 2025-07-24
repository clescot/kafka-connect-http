package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.VersionUtils;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;

public class SseSourceTask extends SourceTask {
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private static final Logger LOGGER = LoggerFactory.getLogger(SseTask.class);
    private SseTask sseTask;

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        this.sseTask = new SseTask(settings);
    }


    @Override
    public List<SourceRecord> poll() {
        Map<String, Queue<SseEvent>> queues = this.sseTask.getQueues();
        List<SourceRecord> records = Lists.newArrayList();
        for (Map.Entry<String,Queue<SseEvent>> queue : queues.entrySet()) {
            records.addAll(poll(queue.getKey(),queue.getValue()));
        }

        return records;
    }

    private List<SourceRecord> poll(String configId,Queue<SseEvent> queue){
        Preconditions.checkNotNull(queue, "queue must not be null.");
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            SseEvent sseEvent = queue.poll();
            LOGGER.debug("Polled from queue: {} event: {} ", sseEvent, configId);
            SourceRecord sourceRecord = new SourceRecord(
                    Maps.newHashMap(),
                    Maps.newHashMap(),
                    this.sseTask.getDefaultTopic(),
                    null,
                    sseEvent.getType(),
                    null,
                    sseEvent.toJson()
            );
            records.add(sourceRecord);
        }
        return records;
    }

    @Override
    public void stop() {
        if (this.sseTask != null) {
            this.sseTask.shutdown();
        }
    }


    public Queue<SseEvent> getQueue(String configurationId) {
        return this.sseTask.getQueue(configurationId);
    }

    public Map<String, Queue<SseEvent>> getQueues() {
        return this.sseTask.getQueues();
    }

    public boolean isConnected(String configurationId) {
        return this.sseTask.isConnected(configurationId);
    }

    public Map<String, SseConfiguration> getConfigurations() {
        return this.sseTask.getConfigurations();
    }

    public SseConfiguration getDefaultConfiguration() {
        Map<String,SseConfiguration> configurations = getConfigurations();
        Preconditions.checkArgument(!configurations.isEmpty(), "Configurations list must not be null or empty.");
        //return the first configuration as default
        return configurations.get(Configuration.DEFAULT_CONFIGURATION_ID);
    }

}
