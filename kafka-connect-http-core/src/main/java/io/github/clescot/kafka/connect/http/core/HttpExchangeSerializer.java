package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Map;

public class HttpExchangeSerializer implements Serializer<HttpExchange> {
    private final ObjectMapper objectMapper;

    public HttpExchangeSerializer() {
         objectMapper= new ObjectMapper();
         objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, HttpExchange data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new ConnectException(e);
        }
    }

}
