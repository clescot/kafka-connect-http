package com.github.clescot.kafka.connect.http.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;

public class KafkaRecord {

    private Iterable<Header> headers;
    private Schema schemaKey;
    private Object key;
    private HttpExchange httpExchange;

    public KafkaRecord(Iterable<Header> headers,
                       Schema schemaKey,
                       Object key,
                       HttpExchange httpExchange) {
        this.headers = headers;
        this.schemaKey = schemaKey;
        this.key = key;
        this.httpExchange = httpExchange;
    }

    public Iterable<Header> getHeaders() {
        return headers;
    }

    public Schema getSchemaKey() {
        return schemaKey;
    }

    public Object getKey() {
        return key;
    }

    public HttpExchange getHttpExchange() {
        return httpExchange;
    }
}
