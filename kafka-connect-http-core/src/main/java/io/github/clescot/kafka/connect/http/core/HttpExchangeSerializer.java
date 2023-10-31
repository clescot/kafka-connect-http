package io.github.clescot.kafka.connect.http.core;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

//content from org.apache.kafka.common.serialization.StringSerializer
public class HttpExchangeSerializer implements Serializer<HttpExchange> {

    private String encoding;

    public HttpExchangeSerializer() {
        this.encoding = StandardCharsets.UTF_8.name();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get("serializer.encoding");
        }

        if (encodingValue instanceof String) {
            this.encoding = (String)encodingValue;
        }
    }

    @Override
    public byte[] serialize(String topic, HttpExchange data) {
        return new byte[0];
    }

    @Override
    public byte[] serialize(String topic, Headers headers, HttpExchange data) {
        try {
            return data == null ? null : data.toString().getBytes(this.encoding);
        } catch (UnsupportedEncodingException var4) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + this.encoding);
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
