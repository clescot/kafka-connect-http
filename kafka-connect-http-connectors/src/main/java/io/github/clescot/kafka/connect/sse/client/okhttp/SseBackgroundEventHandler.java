package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import io.github.clescot.kafka.connect.AbstractClient;
import io.github.clescot.kafka.connect.ResponseClient;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Queue;

import static io.github.clescot.kafka.connect.http.client.HttpClientFactory.CONFIGURATION_ID;

/**
 * SseBackgroundEventHandler is responsible for handling Server-Sent Events (SSE) in the background.
 * It implements the BackgroundEventHandler interface to process events received from an SSE source,
 * and put them into a Queue.
 */
public class SseBackgroundEventHandler extends AbstractClient<SseEvent> implements BackgroundEventHandler, ResponseClient<SseEvent,MessageEvent,SseEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SseBackgroundEventHandler.class);
    private final Queue<SseEvent> queue;
    private final URI uri;


    public SseBackgroundEventHandler(Queue<SseEvent> queue, URI uri) {
        super(Map.of(CONFIGURATION_ID,"sse-background-event-handler"));
        this.queue = queue;
        this.uri = uri;
    }

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

}
