package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.ErrorStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import okhttp3.Request;
import okhttp3.Response;

import java.net.URI;
import java.util.Map;
import java.util.Queue;

public class SseDarklyConfiguration {
    private okhttp3.OkHttpClient internalClient = null;
    private boolean isConnected = false;
    private SseBackgroundEventHandler backgroundEventHandler;

    public SseDarklyConfiguration(Configuration<OkHttpClient, Request, Response> configuration) {
        this.internalClient = configuration.getHttpClient().getInternalClient();

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
        isConnected = true;
        return backgroundEventSource;
    }

    public void shutdown() {
        isConnected = false;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public SseBackgroundEventHandler getBackgroundEventHandler() {
        return backgroundEventHandler;
    }
}
