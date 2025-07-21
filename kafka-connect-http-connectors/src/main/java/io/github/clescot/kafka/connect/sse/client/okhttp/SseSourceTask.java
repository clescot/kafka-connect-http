package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.DEFAULT_CONFIGURATION_ID;

public class SseSourceTask extends SourceTask {
    private static final VersionUtils VERSION_UTILS = new VersionUtils();

    private SseSourceConnectorConfig sseSourceConnectorConfig;
    private SseDarklyConfiguration sseConfiguration;
    private Queue<SseEvent> queue;
    private final ObjectMapper objectMapper;
    private BackgroundEventSource backgroundEventSource;
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
        Map<String,Object> mySettings = Maps.newHashMap(settings);
        Configuration<OkHttpClient, Request, Response> configuration = new Configuration<>(
                DEFAULT_CONFIGURATION_ID,
                new OkHttpClientFactory(),
                mySettings,
                null,
                new CompositeMeterRegistry());
        this.sseConfiguration = new SseDarklyConfiguration(configuration);
        this.queue = QueueFactory.getQueue(String.valueOf(UUID.randomUUID()));
        this.backgroundEventSource = this.sseConfiguration.connect(queue,mySettings);
        this.backgroundEventSource.start();
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
        if(this.backgroundEventSource!=null) {
            this.backgroundEventSource.close();
        }
    }


    public Queue<SseEvent> getQueue() {
        return queue;
    }

    public boolean isConnected() {
        return this.sseConfiguration.isConnected();
    }

    public BackgroundEventSource getBackgroundEventSource() {
        return backgroundEventSource;
    }
}
