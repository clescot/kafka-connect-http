package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;

public class JsonStringDeserializer implements Deserializer<HttpExchange> {

    private final ObjectMapper objectMapper;
    public JsonStringDeserializer() {
        objectMapper= new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }
    @Override
    public HttpExchange deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes,HttpExchange.class);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }
}
