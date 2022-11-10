package com.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Maps;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;

class HttpRequestTest {


    @Test
    public void test_serialization() throws JsonProcessingException, JSONException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                Maps.<String, List<String>>newHashMap(),
                "GET",
                "stuff",
                null,
                null
        );

        String expectedHttpRequest = "{\n" +
                "  \"requestId\": null,\n" +
                "  \"correlationId\": null,\n" +
                "  \"timeoutInMs\": 0,\n" +
                "  \"retries\": 0,\n" +
                "  \"retryDelayInMs\": 0,\n" +
                "  \"retryMaxDelayInMs\": 0,\n" +
                "  \"retryDelayFactor\": 0.0,\n" +
                "  \"retryJitter\": 0,\n" +
                "  \"url\": \"http://www.stuff.com\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"GET\",\n" +
                "  \"bodyAsString\": \"stuff\",\n" +
                "  \"bodyAsByteArray\": \"\",\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"STRING\"\n" +
                "}";

        String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
        JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest,true);
    }

}