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

/**
 * Unit tests for JSONPath functionality used in ExchangeTemplateProcessor
 */
class JsonPathExchangeTemplateProcessorTest {

    /**
     * Test JSONPath expressions against a sample exchange data structure
     */
    @Test
    void testJsonPathExpressions() {
        // Create a test map similar to what createExchangeMap would create
        Map<String, Object> exchangeMap = new HashMap<>();
        
        // Add request
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("url", "http://example.com/api/test");
        requestMap.put("method", "GET");
        exchangeMap.put("request", requestMap);
        
        // Add response
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("statusCode", 200);
        responseMap.put("statusMessage", "OK");
        exchangeMap.put("response", responseMap);
        
        // Configure JSONPath with default configuration for simple maps
        Configuration JSON_PATH_CONFIG = Configuration.defaultConfiguration();
        
        // Test JSONPath expressions
        String[] expressions = {
            "$.response.statusCode",
            "$.request.url",
            "$.response.statusMessage"
        };
        
        Object[] expectedResults = {
            200,
            "http://example.com/api/test",
            "OK"
        };
        
        for (int i = 0; i < expressions.length; i++) {
            String expr = expressions[i];
            Object expected = expectedResults[i];
            
            JsonPath jsonPath = JsonPath.compile(expr);
            Object result = jsonPath.read(exchangeMap, JSON_PATH_CONFIG);
            
            assertThat(result)
                .as("JSONPath expression: " + expr)
                .isEqualTo(expected);
        }
    }
    
    /**
     * Test JSONPath processor supports method
     */
    @Test
    void testJsonPathProcessorSupports() {
        JsonPathExchangeTemplateProcessor processor = new JsonPathExchangeTemplateProcessor();
        
        assertThat(processor.supports("${jsonpath:$.response.statusCode}")).isTrue();
        assertThat(processor.supports("${jsonpath:$.request.url}")).isTrue();
        assertThat(processor.supports("${random.int}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports(null)).isFalse();
    }
    
    /**
     * Test JSONPath processor name
     */
    @Test
    void testJsonPathProcessorName() {
        JsonPathExchangeTemplateProcessor processor = new JsonPathExchangeTemplateProcessor();
        assertThat(processor.getName()).isEqualTo("jsonpath");
    }
}