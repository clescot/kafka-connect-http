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
    private final OkHttpClient client;
    private boolean connected = false;
    private SseBackgroundEventHandler backgroundEventHandler;
    private BackgroundEventSource backgroundEventSource;
    private Queue<SseEvent> queue;

    public SseConfiguration(HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration) {
        this.client = httpClientConfiguration.getClient();
    }

    public static SseConfiguration buildSseConfiguration(Map<String, Object> mySettings, String configurationId) {
        HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>(
                configurationId,
                new OkHttpClientFactory(),
                mySettings,
                null,
                new CompositeMeterRegistry());
        return new SseConfiguration(httpClientConfiguration);
    }

    public BackgroundEventSource connect(Queue<SseEvent> queue, Map<String, Object> settings) {
        Preconditions.checkNotNull(queue, "queue must not be null.");
        this.queue = queue;
        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        Preconditions.checkArgument(!settings.isEmpty(), "settings must not be null or empty.");
        String url = (String) settings.get(SseConfigDefinition.DEFAULT_CONFIG_URL);
        Preconditions.checkNotNull(url, "'url' must not be null or empty.");
        URI uri =  URI.create(url);
        String accessToken = "your_access_token";
        this.backgroundEventHandler = new SseBackgroundEventHandler(queue, uri);
        this.backgroundEventSource = new BackgroundEventSource.Builder(backgroundEventHandler,
                new EventSource.Builder(ConnectStrategy.http(uri)
                        .httpClient(this.client.getInternalClient())
                        //.header("my-application-key", accessToken)
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
