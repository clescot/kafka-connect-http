package io.github.clescot.kafka.connect.sse.client.okhttp;

import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OkHttpEventSourceListener extends EventSourceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(OkHttpEventSourceListener.class);
    private EventSource eventSource;
    private Response response;
    public OkHttpEventSourceListener() {
        super();
    }

    @Override
    public void onOpen(@NotNull EventSource eventSource, @NotNull Response response) {
        super.onOpen(eventSource, response);
        this.eventSource = eventSource;
        this.response = response;
        LOGGER.debug("EventSource opened: {}", eventSource.request().url());
    }

    @Override
    public void onEvent(@NotNull EventSource eventSource, @Nullable String id, @Nullable String type, @NotNull String data) {
        super.onEvent(eventSource, id, type, data);
        LOGGER.debug("Event received: id={}, type={}, data={}", id, type, data);
    }

    @Override
    public void onClosed(@NotNull EventSource eventSource) {
        super.onClosed(eventSource);
        LOGGER.debug("EventSource closed: {}", eventSource.request().url());
    }

    @Override
    public void onFailure(@NotNull EventSource eventSource, @Nullable Throwable t, @Nullable Response response) {
        super.onFailure(eventSource, t, response);
        if (t != null) {
            LOGGER.error("EventSource failure: {}", t.getMessage(), t);
        } else if (response != null) {
            LOGGER.error("EventSource failure with response: {}", response);
        } else {
            LOGGER.error("EventSource failure with no throwable or response");
        }
    }

    public EventSource getEventSource() {
        return eventSource;
    }

    public Response getResponse() {
        return response;
    }
}
