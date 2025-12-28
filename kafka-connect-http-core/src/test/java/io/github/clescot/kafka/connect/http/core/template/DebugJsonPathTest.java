package io.github.clescot.kafka.connect.http.core.template;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class DebugJsonPathTest {

    @Test
    void debugJsonPathEvaluation() {
        // Create test exchange
        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        request.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        request.setBodyAsString("{\"input\": \"test data\"}");
        
        HttpResponse response = new HttpResponse(200, "OK");
        response.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        response.setBodyAsString("{\"result\": \"success\", \"data\": \"processed data\"}");
        
        HttpExchange testExchange = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        // Create the same map structure that JsonPathExchangeTemplateProcessor creates
        Map<String, Object> exchangeMap = new HashMap<>();
        
        // Add basic exchange properties
        exchangeMap.put("durationInMillis", testExchange.getDurationInMillis());
        exchangeMap.put("moment", testExchange.getMoment() != null ? testExchange.getMoment().toString() : null);
        exchangeMap.put("attempts", testExchange.getAttempts() != null ? testExchange.getAttempts().get() : null);
        exchangeMap.put("success", testExchange.isSuccess());
        
        // Add request information
        if (request != null) {
            Map<String, Object> requestMap = new HashMap<>();
            requestMap.put("url", request.getUrl());
            requestMap.put("method", request.getMethod() != null ? request.getMethod().name() : null);
            requestMap.put("headers", request.getHeaders());
            requestMap.put("body", request.getBodyAsString());
            requestMap.put("attributes", request.getAttributes());
            exchangeMap.put("request", requestMap);
        }
        
        // Add response information
        if (response != null) {
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("statusCode", response.getStatusCode());
            responseMap.put("statusMessage", response.getStatusMessage());
            responseMap.put("headers", response.getHeaders());
            responseMap.put("body", response.getBodyAsString());
            responseMap.put("contentType", response.getContentType());
            exchangeMap.put("response", responseMap);
        }
        
        // Add attributes and timings
        exchangeMap.put("attributes", testExchange.getAttributes());
        exchangeMap.put("timings", testExchange.getTimings());

        // Configure JSONPath with Jackson (same as in JsonPathExchangeTemplateProcessor)
        Configuration JSON_PATH_CONFIG = Configuration.builder()
                .jsonProvider(new JacksonJsonNodeJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .options(Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL)
                .build();

        // Test the same expressions as in the test
        String[] expressions = {
            "$.response.statusCode",
            "$.request.url"
        };
        
        System.out.println("Exchange map structure: " + exchangeMap);
        
        for (String expr : expressions) {
            try {
                JsonPath jsonPath = JsonPath.compile(expr);
                Object result = jsonPath.read(exchangeMap, JSON_PATH_CONFIG);
                System.out.println("Expression: " + expr + " -> Result: " + result + " (type: " + (result != null ? result.getClass().getName() : "null") + ")");
                
                // Let's also try some alternative expressions
                String altExpr = expr.replace("$", ""); // Try without the $ prefix
                JsonPath altJsonPath = JsonPath.compile(altExpr);
                Object altResult = altJsonPath.read(exchangeMap, JSON_PATH_CONFIG);
                System.out.println("Alt Expression: " + altExpr + " -> Result: " + altResult + " (type: " + (altResult != null ? altResult.getClass().getName() : "null") + ")");
                
                // Try simple path
                if (expr.contains("response.statusCode")) {
                    JsonPath simplePath = JsonPath.compile("response.statusCode");
                    Object simpleResult = simplePath.read(exchangeMap, JSON_PATH_CONFIG);
                    System.out.println("Simple Expression: response.statusCode -> Result: " + simpleResult);
                }
                
                if (result == null) {
                    System.out.println("DEBUG: Exchange map keys: " + exchangeMap.keySet());
                    if (exchangeMap.containsKey("response")) {
                        System.out.println("DEBUG: Response map: " + exchangeMap.get("response"));
                    }
                }
                
            } catch (Exception e) {
                System.out.println("Expression: " + expr + " -> Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}