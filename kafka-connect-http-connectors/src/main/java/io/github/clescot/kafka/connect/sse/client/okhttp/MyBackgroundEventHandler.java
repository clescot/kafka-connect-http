package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Queue;

public class MyBackgroundEventHandler implements BackgroundEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBackgroundEventHandler.class);
    private final Queue<SseEvent> queue;
    private final URI uri;

    public MyBackgroundEventHandler(Queue<SseEvent> queue, URI uri) {
        this.queue = queue;
        this.uri = uri;
        LOGGER.debug("MyBackgroundEventHandler initialized with queue: {}", queue);
    }

    @Override
    public void onOpen() throws Exception {
        LOGGER.debug("EventSource opened: {}", uri);
    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {

    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {

    }

    public Queue<SseEvent> getQueue() {
        return queue;
    }

    public URI getUri() {
        return uri;
    }
}
