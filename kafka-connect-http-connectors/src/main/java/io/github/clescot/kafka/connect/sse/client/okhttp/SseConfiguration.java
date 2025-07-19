package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

public class SseConfiguration<C extends HttpClient<R, S>, R, S> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SseConfiguration.class);
    private OkHttpSseClient okHttpSseClient;
    private Queue<SseEvent> queue;
    private ObjectMapper objectMapper;

    private final Configuration<C, R, S> configuration;

    public <C extends HttpClient<R, S>,R,S> SseConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Queue<SseEvent> connect(Map<String,String> settings) {
        // Implement connection logic here
        LOGGER.info("Connecting with settings : {}", settings);
        OkHttpClientFactory factory = new OkHttpClientFactory();
        Map<String,Object> config = Maps.newHashMap(settings);
        OkHttpClient okHttpClient = factory.buildHttpClient(config,null,new CompositeMeterRegistry(),  new Random());
        this.queue = QueueFactory.getQueue(""+ UUID.randomUUID());
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        okHttpSseClient = new OkHttpSseClient(okHttpClient.getInternalClient(),queue);
        //Optional<RetryPolicy<HttpExchange>> retryPolicy = configuration.getRetryPolicy();
        //Failsafe.with(policy).with(executorService).getAsync(this::connect);
        okHttpSseClient.connect(settings);
        return queue;
    }

    public void disconnect() {
        if (okHttpSseClient != null) {
            okHttpSseClient.disconnect();
        }
    }

    public void shutdown() {
        disconnect();
        if (okHttpSseClient != null) {
            okHttpSseClient.shutdown();
        }
    }

    public boolean isConnected() {
        return okHttpSseClient != null && okHttpSseClient.isConnected();
    }

}
