package com.github.clescot.kafka.connect.http.sink.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;

public class HeaderImpl implements Header {
    final String key;
    final Schema schema;
    final Object value;

    public HeaderImpl(String key, Schema schema, Object value) {
        this.key = key;
        this.schema = schema;
        this.value = value;
    }

    public String key() {
        return this.key;
    }

    public Schema schema() {
        return this.schema;
    }

    public Object value() {
        return this.value;
    }

    public Header with(Schema schema, Object value) {
        return new HeaderImpl(this.key, schema, value);
    }

    public Header rename(String s) {
        return new HeaderImpl(this.key, this.schema, this.value);
    }
}
