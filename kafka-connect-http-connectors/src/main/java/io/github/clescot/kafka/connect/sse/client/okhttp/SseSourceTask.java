package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;

public class SseSourceTask extends SourceTask {
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private OkHttpSseClient okHttpSseClient;
    private Queue<SseEvent> queue;
    private ObjectMapper objectMapper;
    private SseSourceConnectorConfig sseSourceConnectorConfig;

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings,"settings must not be null or empty.");
        this.sseSourceConnectorConfig = new SseSourceConnectorConfig(settings);
        OkHttpClientFactory factory = new OkHttpClientFactory();
        Map<String,Object> config = Maps.newHashMap(settings);
        OkHttpClient okHttpClient = factory.buildHttpClient(config,null,new CompositeMeterRegistry(),  new Random());
        this.queue = QueueFactory.getQueue(""+ UUID.randomUUID());
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        okHttpSseClient = new OkHttpSseClient(okHttpClient.getInternalClient(),queue);
        okHttpSseClient.connect(settings);
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
        if(okHttpSseClient == null) {
            return;
        }
        okHttpSseClient.shutdown();
    }

    public boolean isConnected() {
        return okHttpSseClient != null && okHttpSseClient.isConnected();
    }

    public Queue<SseEvent> getQueue() {
        return queue;
    }
}
