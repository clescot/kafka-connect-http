package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Queue;

/**
 * SseBackgroundEventHandler is responsible for handling Server-Sent Events (SSE) in the background.
 * It implements the BackgroundEventHandler interface to process events received from an SSE source,
 * and put them into a Queue.
 */
public record SseBackgroundEventHandler(Queue<SseEvent> queue, URI uri) implements BackgroundEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SseBackgroundEventHandler.class);

    @Override
    public void onOpen() throws Exception {
        LOGGER.debug("EventSource opened: {}", uri);
    }

    @Override
    public void onClosed() throws Exception {
        LOGGER.debug("EventSource closed: {}", uri);
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        LOGGER.debug("Event  received type : {}, message:{}", event, messageEvent);
        SseEvent sseEvent = new SseEvent(
                messageEvent.getLastEventId(),
                messageEvent.getEventName(),
                messageEvent.getData()
        );
        queue.add(sseEvent);
    }

    @Override
    public void onComment(String comment) throws Exception {
        LOGGER.debug("comment received: {}", comment);
    }

    @Override
    public void onError(Throwable t) {
        LOGGER.error("Error in EventSource: {}", t.getMessage(), t);
    }
}
