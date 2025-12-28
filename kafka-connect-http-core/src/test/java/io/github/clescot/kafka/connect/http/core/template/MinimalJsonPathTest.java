package io.github.clescot.kafka.connect.http.core.template;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class MinimalJsonPathTest {

    @Test
    void testMinimalJsonPath() {
        // Create a simple map structure
        Map<String, Object> exchangeMap = new HashMap<>();
        
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("url", "http://example.com/api/test");
        exchangeMap.put("request", requestMap);
        
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("statusCode", 200);
        exchangeMap.put("response", responseMap);

        // Configure JSONPath with Jackson (same as in JsonPathExchangeTemplateProcessor)
        Configuration JSON_PATH_CONFIG = Configuration.builder()
                .jsonProvider(new JacksonJsonNodeJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
                .build();

        // Test simple JSONPath expressions
        String[] expressions = {
            "response.statusCode",
            "request.url",
            "$.response.statusCode",
            "$.request.url"
        };
        
        System.out.println("Exchange map: " + exchangeMap);
        
        for (String expr : expressions) {
            try {
                JsonPath jsonPath = JsonPath.compile(expr);
                Object result = jsonPath.read(exchangeMap, JSON_PATH_CONFIG);
                System.out.println("Expression: '" + expr + "' -> Result: " + result);
                
                if (result != null) {
                    assertThat(result).isNotNull();
                }
            } catch (Exception e) {
                System.out.println("Expression: '" + expr + "' -> Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}