package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.ErrorStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;

import java.net.URI;
import java.util.Map;
import java.util.Queue;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.DEFAULT_CONFIGURATION_ID;

public class SseConfiguration {
    private okhttp3.OkHttpClient internalClient = null;
    private boolean connected = false;
    private SseBackgroundEventHandler backgroundEventHandler;

    public SseConfiguration(Configuration<OkHttpClient, Request, Response> configuration) {
        this.internalClient = configuration.getHttpClient().getInternalClient();

    }

    public static SseConfiguration buildSseConfiguration(Map<String, Object> mySettings) {
        Configuration<OkHttpClient, Request, Response> configuration = new Configuration<>(
                DEFAULT_CONFIGURATION_ID,
                new OkHttpClientFactory(),
                mySettings,
                null,
                new CompositeMeterRegistry());
        return new SseConfiguration(configuration);
    }

    public BackgroundEventSource connect(Queue<SseEvent> sseEventQueue, Map<String, Object> settings) {
        URI uri =  URI.create((String) settings.get("url"));
        String accessToken = "your_access_token";
        this.backgroundEventHandler = new SseBackgroundEventHandler(sseEventQueue, uri);
        BackgroundEventSource backgroundEventSource = new BackgroundEventSource.Builder(backgroundEventHandler,
                new EventSource.Builder(ConnectStrategy.http(uri)
                        .httpClient(internalClient)
                        //.header("hue-application-key", accessToken)
                )
                        .errorStrategy(ErrorStrategy.alwaysContinue())
        ).build();
        connected = true;
        return backgroundEventSource;
    }

    public void shutdown() {
        connected = false;
    }

    public boolean isConnected() {
        return connected;
    }


    public SseBackgroundEventHandler getBackgroundEventHandler() {
        return backgroundEventHandler;
    }
}
