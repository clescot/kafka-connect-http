package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.VersionUtils;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class SseSourceTask extends SourceTask {
    private static final VersionUtils VERSION_UTILS = new VersionUtils();

    private BackgroundEventSource backgroundEventSource;
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
        Collection<Queue<SseEvent>> queues = this.sseTask.getQueues();
        List<SourceRecord> records = Lists.newArrayList();
        for (Queue<SseEvent> queue : queues) {
            records.addAll(poll(queue));
        }

        return records;
    }

    private List<SourceRecord> poll(Queue<SseEvent> queue){
        Preconditions.checkNotNull(queue, "queue must not be null.");
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            SseEvent sseEvent = queue.poll();
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
        if (this.backgroundEventSource != null) {
            this.backgroundEventSource.close();
        }
    }


    public Queue<SseEvent> getQueue(String configurationId) {
        return this.sseTask.getQueue(configurationId);
    }

    public boolean isConnected(String configurationId) {
        return this.sseTask.isConnected(configurationId);
    }

    public BackgroundEventSource getBackgroundEventSource() {
        return backgroundEventSource;
    }
}
