package io.github.clescot.core.template;

import io.github.clescot.core.http.template.*;
import io.github.clescot.core.http.Exchange;
import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
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
        
        // Verify that the processor returned content with JSONPath results
        assertThat(processedExchange).isNotNull();
        String content = httpProcessedExchange.getContent();
        assertThat(content).isNotEmpty();
        assertThat(content).contains("200");
        assertThat(content).contains("http://example.com/api/test");
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
        
        // Verify that the processor returned content with random values
        assertThat(httpProcessedExchange).isNotNull();
        String content = httpProcessedExchange.getContent();
        assertThat(content).isNotEmpty();
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
        String resolvedTemplate = manager.resolveTemplate(testExchange, jsonPathTemplate, new HashMap<>());
        assertThat(resolvedTemplate).isEqualTo("200");
        
        // Test Random template
        String randomTemplate = "${random.int:1:100}";
        String resolvedTemplate2 = manager.resolveTemplate(testExchange, randomTemplate, new HashMap<>());
        assertThat(resolvedTemplate2).isNotEmpty();
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