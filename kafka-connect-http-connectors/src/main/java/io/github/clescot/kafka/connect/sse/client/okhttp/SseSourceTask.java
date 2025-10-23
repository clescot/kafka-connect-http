package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;

import static io.github.clescot.kafka.connect.http.core.VersionUtils.VERSION;

public class SseSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(SseSourceTask.class);
    private SseTask sseTask;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        this.sseTask = new SseTask(settings);
        this.sseTask.connect();
        this.sseTask.start();
    }


    @Override
    public List<SourceRecord> poll() {
        Map<String, Queue<SseEvent>> queues = this.sseTask.getQueues();
        Map<String, SseConfiguration> configurations = this.sseTask.getConfigurations();
        List<SourceRecord> records = Lists.newArrayList();
        for (Map.Entry<String,Queue<SseEvent>> queue : queues.entrySet()) {
            records.addAll(poll(configurations.get(queue.getKey()),queue.getValue()));
        }

        return records;
    }

    private List<SourceRecord> poll(SseConfiguration sseConfiguration,Queue<SseEvent> queue){
        Preconditions.checkNotNull(queue, "queue must not be null.");
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            SseEvent sseEvent = queue.poll();
            LOGGER.debug("Polled from queue: {} event: {} ", sseEvent, sseConfiguration.getConfigurationId());
            Map<String, String> sourcePartition = sseEvent.getType()!=null?Map.of("type",sseEvent.getType()):Maps.newHashMap();
            Map<String, String> sourceOffset = sseEvent.getId()!=null?Map.of("eventId", sseEvent.getId()):Maps.newHashMap();
            SourceRecord sourceRecord = new SourceRecord(
                    sourcePartition,
                    sourceOffset,
                    sseConfiguration.getTopic(),
                    Schema.STRING_SCHEMA,
                    sseEvent.getType(),
                    SseEvent.SCHEMA,
                    sseEvent.toJson()
            );
            records.add(sourceRecord);
        }
        return records;
    }

    @Override
    public void stop() {
        if (this.sseTask != null) {
            this.sseTask.stop();
        }
    }


    public Optional<Queue<SseEvent>> getQueue(String configurationId) {
        return this.sseTask.getQueue(configurationId);
    }

    public Map<String, Queue<SseEvent>> getQueues() {
        return this.sseTask.getQueues();
    }

    public boolean isConnected(String configurationId) {
        Preconditions.checkNotNull(this.sseTask, "sseTask must not be null. call 'start()' method first.");
        return this.sseTask.isConnected(configurationId);
    }

    public Map<String, SseConfiguration> getConfigurations() {
        return this.sseTask.getConfigurations();
    }


}
