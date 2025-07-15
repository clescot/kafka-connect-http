package io.github.clescot.kafka.connect.sse.core;

import java.io.Serial;
import java.io.Serializable;

public class SseEvent implements Cloneable, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final String id;
    private final String type;
    private final String data;

    public SseEvent(String id, String type, String data) {
        this.id = id;
        this.type = type;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getData() {
        return data;
    }
}
