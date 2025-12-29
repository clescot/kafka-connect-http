package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ExchangeTemplateProcessorTest {

    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        // Create a test HttpExchange
        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        request.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        request.setBodyAsString("{\"input\": \"test data\"}");
        
        HttpResponse response = new HttpResponse(200, "OK");
        response.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        response.setBodyAsString("{\"result\": \"success\", \"data\": \"processed data\"}");
        
        testExchange = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );
    }

    @Test
    void testJsonPathProcessor() {
        JsonPathExchangeTemplateProcessor processor = new JsonPathExchangeTemplateProcessor();
        
        // Test template with JSONPath expressions
        String template = "${jsonpath:$.response.statusCode} ${jsonpath:$.request.url}";
        
        assertThat(processor.supports(template)).isTrue();
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Verify that the processor added attributes with JSONPath results
        assertThat(processedExchange).isNotNull();
        assertThat(processedExchange.getAttributes()).isNotEmpty();
        assertThat(processedExchange.getAttributes()).containsKey("jsonpath___response_statusCode");
        assertThat(processedExchange.getAttributes()).containsKey("jsonpath___request_url");
        
        // Verify the values are correct
        assertThat(processedExchange.getAttributes().get("jsonpath___response_statusCode")).isEqualTo("200");
        assertThat(processedExchange.getAttributes().get("jsonpath___request_url")).isEqualTo("http://example.com/api/test");
    }

    @Test
    void testRandomProcessor() {
        RandomExchangeTemplateProcessor processor = new RandomExchangeTemplateProcessor();
        
        // Test template with random value expressions
        String template = "${random.int:1:100} ${random.uuid} ${random.string:5}";
        
        assertThat(processor.supports(template)).isTrue();
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Verify that the processor added random value attributes
        assertThat(httpProcessedExchange).isNotNull();
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        
        // Check that we have at least one random attribute
        boolean hasRandomAttribute = httpProcessedExchange.getAttributes().keySet().stream()
                .anyMatch(key -> key.startsWith("random_"));
        assertThat(hasRandomAttribute).isTrue();
    }

    @Test
    void testTemplateManager() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
        
        // Register processors
        manager.registerProcessor(new JsonPathExchangeTemplateProcessor());
        manager.registerProcessor(new RandomExchangeTemplateProcessor());
        
        assertThat(manager.getProcessors()).hasSize(2);
        assertThat(manager.canProcess("${jsonpath:$.test}")).isTrue();
        assertThat(manager.canProcess("${random.int}")).isTrue();
        assertThat(manager.canProcess("plain text")).isFalse();
    }

    @Test
    void testTemplateManagerProcessing() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager();
        manager.registerProcessor(new JsonPathExchangeTemplateProcessor());
        manager.registerProcessor(new RandomExchangeTemplateProcessor());
        
        // Test JSONPath template
        String jsonPathTemplate = "${jsonpath:response.statusCode}";
        Exchange<?, ?> processedJsonPath = manager.processTemplate(testExchange, jsonPathTemplate, new HashMap<>());
        assertThat(processedJsonPath).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedJsonPath = (HttpExchange) processedJsonPath;
        assertThat(httpProcessedJsonPath.getAttributes()).containsKey("jsonpath_response_statusCode");
        assertThat(httpProcessedJsonPath.getAttributes().get("jsonpath_response_statusCode")).isEqualTo("200");
        
        // Test Random template
        String randomTemplate = "${random.int:1:100}";
        Exchange<?, ?> processedRandom = manager.processTemplate(testExchange, randomTemplate, new HashMap<>());
        assertThat(processedRandom).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedRandom = (HttpExchange) processedRandom;
        boolean hasRandomAttribute = httpProcessedRandom.getAttributes().keySet().stream()
                .anyMatch(key -> key.startsWith("random_"));
        assertThat(hasRandomAttribute).isTrue();
    }

    @Test
    void testProcessorFactory() {
        ExchangeTemplateProcessorFactory factory = new ExchangeTemplateProcessorFactory();
        
        // Test creating built-in processors
        ExchangeTemplateProcessor jsonPathProcessor = factory.createBuiltinProcessor("jsonpath");
        assertThat(jsonPathProcessor).isInstanceOf(JsonPathExchangeTemplateProcessor.class);
        
        ExchangeTemplateProcessor randomProcessor = factory.createBuiltinProcessor("random");
        assertThat(randomProcessor).isInstanceOf(RandomExchangeTemplateProcessor.class);
        
        // Test unknown processor
        try {
            factory.createBuiltinProcessor("unknown");
            org.junit.jupiter.api.Assertions.fail("Should throw exception for unknown processor");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("Unknown built-in processor");
        }
    }

    @Test
    void testDefaultTemplateManager() {
        ExchangeTemplateProcessorFactory factory = new ExchangeTemplateProcessorFactory();
        ExchangeTemplateManager manager = factory.createDefaultTemplateManager();
        
        // Should have at least the built-in processors
        assertThat(manager.getProcessors()).hasSizeGreaterThanOrEqualTo(2);
        
        // Should be able to process both JSONPath and Random templates
        assertThat(manager.canProcess("${jsonpath:$.test}")).isTrue();
        assertThat(manager.canProcess("${random.int}")).isTrue();
    }
}