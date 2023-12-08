package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class HttpExchangeDeserializer implements Deserializer<HttpExchange> {
    private final ObjectMapper objectMapper;

    public HttpExchangeDeserializer() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public HttpExchange deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, HttpExchange.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
