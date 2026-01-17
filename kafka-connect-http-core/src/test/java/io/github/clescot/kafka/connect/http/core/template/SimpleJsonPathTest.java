package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleJsonPathTest {

    @Test
    void testSimpleJsonPath() {
        System.out.println("DEBUG: Starting simple JSONPath test");
        
        // Create a simple test exchange
        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        HttpResponse response = new HttpResponse(200, "OK");
        
        HttpExchange testExchange = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        // Create processor
        JsonPathExchangeTemplateProcessor processor = new JsonPathExchangeTemplateProcessor();
        System.out.println("DEBUG: Created processor");
        
        // Test simple template
        String template = "${jsonpath:$.response.statusCode}";
        System.out.println("DEBUG: Template: " + template);
        
        // Test supports
        boolean supports = processor.supports(template);
        System.out.println("DEBUG: supports result: " + supports);
        assertThat(supports).isTrue();
        
        // Test process
        System.out.println("DEBUG: Calling process method");
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        System.out.println("DEBUG: Process completed");
        
        // Check results
        assertThat(httpProcessedExchange).isNotNull();
        System.out.println("DEBUG: Processed exchange content: " + httpProcessedExchange.getContent());
        assertThat(httpProcessedExchange.getContent()).isNotEmpty();
        
        System.out.println("DEBUG: Test completed successfully");
    }
}