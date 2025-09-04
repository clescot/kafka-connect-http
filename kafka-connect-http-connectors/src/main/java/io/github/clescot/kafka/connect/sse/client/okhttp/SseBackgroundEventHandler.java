package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.ResponseClient;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Optional;
import java.util.Queue;

/**
 * SseBackgroundEventHandler is responsible for handling Server-Sent Events (SSE) in the background.
 * It implements the BackgroundEventHandler interface to process events received from an SSE source,
 * and put them into a Queue.
 */
public record SseBackgroundEventHandler(Queue<SseEvent> queue, URI uri) implements BackgroundEventHandler, ResponseClient<SseEvent,MessageEvent> {
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
        queue.add(buildResponse(messageEvent));
    }

    @Override
    public void onComment(String comment) throws Exception {
        LOGGER.debug("comment received: {}", comment);
    }

    @Override
    public void onError(Throwable t) {
        LOGGER.error("Error in EventSource: {}", t.getMessage(), t);
    }

    @Override
    public SseEvent buildResponse(MessageEvent response) {
        return new SseEvent(
                response.getLastEventId(),
                response.getEventName(),
                response.getData()
        );
    }

    @Override
    public String getEngineId() {
        return "okhttp-sse";
    }

    @Override
    public Optional<RateLimiter<HttpExchange>> getRateLimiter() {
        return Optional.empty();
    }
}
