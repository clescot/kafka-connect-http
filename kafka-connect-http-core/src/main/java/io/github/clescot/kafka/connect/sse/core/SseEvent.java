package io.github.clescot.kafka.connect.sse.core;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a Server-Sent Event (SSE) with an ID, type, and data.
 * This class is serializable and can be cloned.
 */
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SseEvent sseEvent = (SseEvent) o;
        return Objects.equals(getId(), sseEvent.getId()) && Objects.equals(getType(), sseEvent.getType()) && Objects.equals(getData(), sseEvent.getData());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getType(), getData());
    }

    @Override
    public SseEvent clone() {
        try {
            return (SseEvent) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public String toString() {
        return "SseEvent{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
