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
    private final OkHttpClient okHttpClient;
    private boolean isConnected = false;
    private final EventSource.Factory factory;
    private EventSource eventSource;
    private final OkHttpEventSourceListener eventSourceListener;

    public OkHttpSseClient(OkHttpClient okHttpClient, Queue<SseEvent> queue) {
        this.okHttpClient = okHttpClient;
        this.factory = EventSources.createFactory(okHttpClient);
        eventSourceListener = new OkHttpEventSourceListener(queue);
    }
    /**
     * Connects to the SSE server using the provided configuration.
     * The configuration should contain the URL of the SSE endpoint.
     *
     * @param config A map containing the configuration settings, including the `url` parameter.
     */
    public void connect(Map<String, String> config) {
        String url = config.get("url");
        Request request = new Request.Builder()
                .url(url)
                .build();
        // Create the EventSource with the provided listener

        eventSource = factory.newEventSource(request, eventSourceListener);
        isConnected = true;
    }
    /**
     * Disconnects from the SSE server.
     * This method cancels the EventSource connection and sets the isConnected flag to false.
     */
    public void disconnect() {
        // Logic to disconnect from the SSE server
        if(eventSource != null) {
            eventSource.cancel();
        }
        isConnected = false;
    }
    /**
     * Disconnects from the SSE server and shuts down the OkHttp client.
     */
    public void shutdown() {
        disconnect();
        okHttpClient.dispatcher().executorService().shutdown();
    }

    public boolean isConnected() {
        return isConnected;
    }

    public Queue<SseEvent> getEventQueue() {
        return eventSourceListener.getQueue();
    }
}
