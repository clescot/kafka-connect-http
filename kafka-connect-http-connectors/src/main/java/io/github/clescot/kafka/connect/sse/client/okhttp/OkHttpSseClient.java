package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.sse.core.SseEvent;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSources;

import java.util.Map;
import java.util.Queue;

/**
 * This class represents a client for Server-Sent Events (SSE) with the OkHttp library.
 * It is designed to handle connections to an SSE server.
 */
public class OkHttpSseClient {
    private boolean isConnected = false;
    private final EventSource.Factory factory;
    private EventSource eventSource;
    private final OkHttpEventSourceListener eventSourceListener;

    public OkHttpSseClient(OkHttpClient okHttpClient, Queue<SseEvent> queue) {
        this.factory = EventSources.createFactory(okHttpClient);
        eventSourceListener = new OkHttpEventSourceListener(queue);
    }

    public void connect(Map<String, String> config) {
        String url = config.get("url");
        Request request = new Request.Builder()
                .url(url)
                .build();
        // Create the EventSource with the provided listener

        eventSource = factory.newEventSource(request, eventSourceListener);
        isConnected = true;
    }

    public void disconnect() {
        // Logic to disconnect from the SSE server
        eventSource.cancel();
        isConnected = false;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public Queue<SseEvent> getEventQueue() {
        return eventSourceListener.getQueue();
    }
}
