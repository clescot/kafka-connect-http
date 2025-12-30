package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test array processing capabilities with SSE events
 */
@Execution(ExecutionMode.SAME_THREAD)
class SseArrayProcessingTest {

    private OkHttpClient mockHttpClient;

    @BeforeEach
    void setup() {
        QueueFactory.clearRegistrations();
        QueueFactory.clearQueueMap();
        mockHttpClient = Mockito.mock(OkHttpClient.class);
    }

    @Test
    void test_array_processing_with_jmespath_ids() {
        // Create SSE event with array data
        SseEvent originalEvent = new SseEvent("test-id", "test-type",
                "{\"items\": [{\"id\": 1, \"name\": \"Item1\"}, {\"id\": 2, \"name\": \"Item2\"}, {\"id\": 3, \"name\": \"Item3\"}]}");

        // Create configuration with array processing template
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "array-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");

        // Template to extract all item IDs from array using JmesPath
        // This will create an attribute 'extracted_ids' with the result
        settings.put("exchange.template", "${jmespath:items[*].id:extracted_ids}");

        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);

        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);

        // Verify processing worked
        assertThat(processedEvent).isNotNull();
        // Check that the attribute contains the extracted IDs
        assertThat(processedEvent.getAttributes()).containsKey("extracted_ids");
        assertThat(processedEvent.getAttributes().get("extracted_ids")).asString().contains("[1,2,3]");
    }

    @Test
    void test_array_processing_with_specific_elements() {
        // Create SSE event with array data
        SseEvent originalEvent = new SseEvent("test-id", "test-type",
                "{\"users\": [{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}, {\"id\": 3, \"name\": \"Charlie\"}]}");

        // Create configuration with specific element extraction
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "specific-array-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");

        // Template to extract first and last user names
        // JmesPath slice: [0] and [-1] or multi-select [0, 2].name
        settings.put("exchange.template", "${jmespath:users[0,2].name:extracted_users}");

        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);

        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);

        // Verify processing worked
        assertThat(processedEvent).isNotNull();
        assertThat(processedEvent.getAttributes()).containsKey("extracted_users");
        assertThat(processedEvent.getAttributes().get("extracted_users")).asString().contains("Alice");
        assertThat(processedEvent.getAttributes().get("extracted_users")).asString().contains("Charlie");
    }

    @Test
    void test_array_processing_with_filtering() {
        // Create SSE event with array data
        SseEvent originalEvent = new SseEvent("test-id", "test-type",
                "{\"products\": [{\"id\": 1, \"price\": 10.99, \"active\": true}, {\"id\": 2, \"price\": 25.50, \"active\": false}, {\"id\": 3, \"price\": 15.75, \"active\": true}]}");

        // Create configuration with filtering
        Map<String, String> settings = Maps.newHashMap();
        String configurationId = "filter-array-test";
        settings.put("config.ids", configurationId);
        settings.put("topic", "test-topic");
        settings.put("url", "http://example.com/sse");

        // Template to extract only active products' prices
        settings.put("exchange.template", "${jmespath:products[?(@.active == true)].price:active_prices}");

        // Create configuration
        SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);

        // Process the event
        HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
        SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);

        // Verify processing worked
        assertThat(processedEvent).isNotNull();
        assertThat(processedEvent.getAttributes()).containsKey("active_prices");
        assertThat(processedEvent.getAttributes().get("active_prices")).asString().contains("10.99");
        assertThat(processedEvent.getAttributes().get("active_prices")).asString().contains("15.75");
        assertThat(processedEvent.getAttributes().get("active_prices")).asString().doesNotContain("25.50");
    }
}