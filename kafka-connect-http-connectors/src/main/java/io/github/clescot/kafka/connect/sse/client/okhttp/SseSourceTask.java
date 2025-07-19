package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;
import java.util.Queue;

public class SseSourceTask extends SourceTask {
    private static final VersionUtils VERSION_UTILS = new VersionUtils();

    private SseSourceConnectorConfig sseSourceConnectorConfig;
    private SseConfiguration<OkHttpClient, Request, Response> sseConfiguration;
    private Queue<SseEvent> queue;
    private final ObjectMapper objectMapper;

    public SseSourceTask() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        this.sseSourceConnectorConfig = new SseSourceConnectorConfig(settings);
        Configuration<OkHttpClient, Request, Response> configuration = null;
        this.sseConfiguration = new SseConfiguration<>(configuration);
        this.queue = this.sseConfiguration.connect(settings);
    }

    @Override
    public List<SourceRecord> poll() {

        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            SseEvent sseEvent = queue.poll();
            SourceRecord sourceRecord;
            try {
                sourceRecord = new SourceRecord(
                        Maps.newHashMap(),
                        Maps.newHashMap(),
                        sseSourceConnectorConfig.getTopic(),
                        null,
                        objectMapper.writeValueAsString(sseEvent)
                );
            } catch (JsonProcessingException e) {
                throw new SseException(e);
            }
            records.add(sourceRecord);
        }
        return records;
    }

    @Override
    public void stop() {
        if (this.sseConfiguration != null) {
            this.sseConfiguration.shutdown();
        }
    }


    public Queue<SseEvent> getQueue() {
        return queue;
    }

    public boolean isConnected() {
        return this.sseConfiguration.isConnected();
    }
}
