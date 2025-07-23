package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import static io.github.clescot.kafka.connect.sse.client.okhttp.SseConfiguration.buildSseConfiguration;

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
        Queue<SseEvent> queue = this.sseTask.getQueue();
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            SseEvent sseEvent = queue.poll();
            SourceRecord sourceRecord = new SourceRecord(
                    Maps.newHashMap(),
                    Maps.newHashMap(),
                    this.sseTask.getTopic(),
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


    public Queue<SseEvent> getQueue() {
        return this.sseTask.getQueue();
    }

    public boolean isConnected() {
        return this.sseTask.isConnected();
    }

    public BackgroundEventSource getBackgroundEventSource() {
        return backgroundEventSource;
    }
}
