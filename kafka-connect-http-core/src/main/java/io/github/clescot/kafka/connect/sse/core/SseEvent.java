package io.github.clescot.kafka.connect.sse.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.Response;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a Server-Sent Event (SSE) with an ID, type, and data.
 * This class is serializable and can be cloned.
 */
public class SseEvent implements Response,Cloneable, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    @JsonProperty
    private Map<String,Object> attributes = Maps.newHashMap();

    @JsonProperty
    private String id;
    @JsonProperty
    private String type;
    @JsonProperty
    private String data;

    public static final int VERSION = 1;

    private static final String ID = "id";
    private static final String TYPE = "type";
    private static final String DATA = "data";
    public static final String ATTRIBUTES = "attributes";
    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(SseEvent.class.getName())
            .version(VERSION)
            .field(ID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(ATTRIBUTES, SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).optional().schema())
            .field(TYPE, Schema.STRING_SCHEMA)
            .field(DATA, Schema.STRING_SCHEMA)
            .schema();

    protected SseEvent() {}

    public SseEvent(String id, String type, String data) {
        this.id = id;
        this.type = type;
        this.data = data;
    }

    public SseEvent(Struct struct) {
        this.id = struct.getString(ID);
        this.type = struct.getString(TYPE);
        this.data = struct.getString(DATA);
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
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    protected void setId(String id) {
        this.id = id;
    }

    protected void setType(String type) {
        this.type = type;
    }

    protected void setData(String data) {
        this.data = data;
    }

    protected void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
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
                ", attributes='" + attributes + '\'' +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                '}';
    }

    public String toJson() {
        return "{\"id\":\"" + id +
                "\",\"attributes\":\"" + attributes +
                "\",\"type\":\"" + type +
                "\",\"data\":\"" + data +
                "\"}";
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(ID, id)
                .put(ATTRIBUTES, this.getAttributes())
                .put(TYPE, type)
                .put(DATA, data)
                ;
    }


}
