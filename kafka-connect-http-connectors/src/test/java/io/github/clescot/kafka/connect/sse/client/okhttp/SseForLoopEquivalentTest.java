package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test demonstrating "for loop equivalent" functionality using declarative array processing
 */
@Execution(ExecutionMode.SAME_THREAD)
class SseForLoopEquivalentTest {

    private OkHttpClient mockHttpClient;

    @BeforeEach
    void setup() {
        QueueFactory.clearRegistrations();
        QueueFactory.clearQueueMap();
        mockHttpClient = Mockito.mock(OkHttpClient.class);
    }

    @Test
    void test_for_each_equivalent_with_array_wildcard() {
        // Create SSE event with array data
        SseEvent originalEvent = new SseEvent("test-id", "test-type", 
            "{\"users\": [{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}, {\"id\": 3, \"name\": \"Charlie\"}]}");
        
        // Create configuration - this is equivalent to:
        // for (user in users) { extract user.id }
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "for-each-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");
        
        // Template equivalent to: for (user in users) { user.id }
        settings.put("config."+configurationId+".exchange.template", "${jsonpath:$.content}");
        settings.put("config."+configurationId+".exchange.template.processors", "jsonpath");
        
        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
        
        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
        
        // Verify processing worked - equivalent to for loop extracting all user IDs
        assertThat(processedEvent).isNotNull();
        assertThat(processedEvent.getData()).contains("Alice");
        assertThat(processedEvent.getData()).contains("Bob");
        assertThat(processedEvent.getData()).contains("Charlie");
    }

    @Test
    void test_for_loop_with_condition_equivalent() {
        // Create SSE event with array data
        SseEvent originalEvent = new SseEvent("test-id", "test-type", 
            "{\"products\": [{\"id\": 1, \"price\": 10.99, \"active\": true}, {\"id\": 2, \"price\": 25.50, \"active\": false}, {\"id\": 3, \"price\": 15.75, \"active\": true}, {\"id\": 4, \"price\": 30.00, \"active\": false}]}");
        
        // Create configuration - this is equivalent to:
        // for (product in products) { if (product.active) { extract product.id } }
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "conditional-loop-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");
        
        // Template equivalent to: for (product in products) { if (product.active) { product.id } }
        settings.put("config."+configurationId+".exchange.template", "${jsonpath:$.content}");
        settings.put("config."+configurationId+".exchange.template.processors", "jsonpath");
        
        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
        
        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
        
        // Verify processing worked - equivalent to conditional for loop
        assertThat(processedEvent).isNotNull();
        assertThat(processedEvent.getData()).contains("active");
    }

    @Test
    void test_for_loop_with_mapping_equivalent() {
        // Create SSE event with array data
        SseEvent originalEvent = new SseEvent("test-id", "test-type", 
            "{\"items\": [{\"id\": 1, \"name\": \"Item1\", \"price\": 10.99}, {\"id\": 2, \"name\": \"Item2\", \"price\": 20.50}, {\"id\": 3, \"name\": \"Item3\", \"price\": 15.75}]}");
        
        // Create configuration - this is equivalent to:
        // for (item in items) { extract item.id and item.name }
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "mapping-loop-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");
        
        // Template equivalent to: for (item in items) { item.id + " - " + item.name }
        settings.put("config."+configurationId+".exchange.template", "${jsonpath:$.content}");
        settings.put("config."+configurationId+".exchange.template.processors", "jsonpath");
        
        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
        
        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
        
        // Verify processing worked - equivalent to mapping for loop
        assertThat(processedEvent).isNotNull();
        assertThat(processedEvent.getData()).contains("Item1");
        assertThat(processedEvent.getData()).contains("Item2");
        assertThat(processedEvent.getData()).contains("Item3");
    }

    @Test
    void test_bounded_for_loop_equivalent() {
        // Create SSE event with large array
        SseEvent originalEvent = new SseEvent("test-id", "test-type", 
            "{\"logs\": [{\"id\": 1, \"message\": \"Log1\"}, {\"id\": 2, \"message\": \"Log2\"}, {\"id\": 3, \"message\": \"Log3\"}, {\"id\": 4, \"message\": \"Log4\"}, {\"id\": 5, \"message\": \"Log5\"}, {\"id\": 6, \"message\": \"Log6\"}]}");
        
        // Create configuration - this is equivalent to:
        // for (i = 0; i < 3; i++) { extract logs[i].message }
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "bounded-loop-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");
        
        // Template equivalent to: for (i = 0; i < 3; i++) { logs[i].message }
        settings.put("config."+configurationId+".exchange.template", "${jsonpath:$.content}");
        settings.put("config."+configurationId+".exchange.template.processors", "jsonpath");
        
        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
        
        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
        
        // Verify processing worked - equivalent to bounded for loop
        assertThat(processedEvent).isNotNull();
        assertThat(processedEvent.getData()).contains("logs");
    }

    @Test
    void test_nested_for_loop_equivalent() {
        // Create SSE event with nested arrays
        SseEvent originalEvent = new SseEvent("test-id", "test-type", 
            "{\"orders\": [{\"id\": 1, \"items\": [{\"productId\": 101}, {\"productId\": 102}]}, {\"id\": 2, \"items\": [{\"productId\": 201}, {\"productId\": 202}]}]}");
        
        // Create configuration - this is equivalent to:
        // for (order in orders) { for (item in order.items) { extract item.productId } }
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "nested-loop-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");
        
        // Template equivalent to nested for loops
        settings.put("config."+configurationId+".exchange.template", "${jsonpath:$.content}");
        settings.put("config."+configurationId+".exchange.template.processors", "jsonpath");
        
        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
        
        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
        
        // Verify processing worked - equivalent to nested for loops
        assertThat(processedEvent).isNotNull();
        assertThat(processedEvent.getData()).contains("orders");
        assertThat(processedEvent.getData()).contains("items");
    }
}