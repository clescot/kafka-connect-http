package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.ErrorStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.RetryDelayStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;

import java.net.URI;
import java.util.Map;
import java.util.Queue;


public class SseConfiguration implements Configuration<OkHttpClient, HttpRequest> {
    private final String configurationId;
    private final OkHttpClient client;
    private final Map<String, Object> settings;
    private boolean connected = false;
    private SseBackgroundEventHandler backgroundEventHandler;
    private BackgroundEventSource backgroundEventSource;
    private Queue<SseEvent> queue;

    public SseConfiguration(String configurationId, HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration, Map<String, Object> settings) {
        this.configurationId = configurationId;
        this.client = httpClientConfiguration.getClient();
        this.settings = settings;
    }

    public static SseConfiguration buildSseConfiguration(String configurationId, Map<String, Object> mySettings) {
        HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>(
                configurationId,
                new OkHttpClientFactory(),
                mySettings,
                null,
                new CompositeMeterRegistry());
        return new SseConfiguration(configurationId,httpClientConfiguration,mySettings);
    }

    public BackgroundEventSource connect(Queue<SseEvent> queue) {
        Preconditions.checkNotNull(queue, "queue must not be null.");
        this.queue = queue;
        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        Preconditions.checkArgument(!settings.isEmpty(), "settings must not be null or empty.");
        String url = (String) settings.get(SseConfigDefinition.URL);
        Preconditions.checkNotNull(url, "'url' must not be null or empty.");
        URI uri =  URI.create(url);
        RetryDelayStrategy retryDelayStrategy = RetryDelayStrategy.defaultStrategy();
        if (settings.containsKey(SseConfigDefinition.RETRY_DELAY_STRATEGY)) {

        }
        this.backgroundEventHandler = new SseBackgroundEventHandler(queue, uri);
        this.backgroundEventSource = new BackgroundEventSource.Builder(backgroundEventHandler,
                new EventSource.Builder(ConnectStrategy.http(uri)
                        .httpClient(this.client.getInternalClient())
                )
                        .streamEventData(false)
                        .retryDelayStrategy(RetryDelayStrategy.defaultStrategy())
                        .errorStrategy(ErrorStrategy.alwaysContinue())
        ).build();
        connected = true;
        return backgroundEventSource;
    }

    public void start() {
        Preconditions.checkState(connected, "SSE client is not connected. Call connect() first.");
        if (backgroundEventSource != null) {
            backgroundEventSource.start();
        }
    }

    public void shutdown() {
        connected = false;
        if (backgroundEventSource != null) {
            backgroundEventSource.close();
        }
    }

    public boolean isConnected() {
        return connected;
    }


    public SseBackgroundEventHandler getBackgroundEventHandler() {
        return backgroundEventHandler;
    }

    @Override
    public boolean matches(HttpRequest request) {
        return false;
    }

    @Override
    public OkHttpClient getClient() {
        return client;
    }

    public Queue<SseEvent> getQueue() {
        return queue;
    }

    public BackgroundEventSource getBackgroundEventSource() {
        return backgroundEventSource;
    }
}
