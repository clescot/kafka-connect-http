package io.github.clescot.kafka.connect.sse.client.okhttp;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;

import java.util.Map;

/**
 * This class represents a client for Server-Sent Events (SSE) with the OkHttp library.
 * It is designed to handle connections to an SSE server.
 */
public class OkHttpSseClient {

    private final EventSource.Factory factory;
    private EventSource eventSource;
    private final String url;
    public OkHttpSseClient(Map<String, Object> config,OkHttpClient okHttpClient) {
        this.factory = EventSources.createFactory(okHttpClient);
        this.url = (String) config.get("sse.url");

    }

    public void connect() {
        Request request = new Request.Builder()
                .url(url)
                .build();

        // Create the EventSource with the provided listener
        eventSource = factory.newEventSource(request, new OkHttpEventSourceListener());
    }

    public void disconnect() {
        // Logic to disconnect from the SSE server
        eventSource.cancel();
    }

}
