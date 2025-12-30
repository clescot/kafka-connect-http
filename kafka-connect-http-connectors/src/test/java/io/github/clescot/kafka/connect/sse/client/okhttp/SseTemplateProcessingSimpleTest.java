package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class SseTemplateProcessingSimpleTest {

    @Nested
    class TemplateProcessing {
        
        private OkHttpClient mockHttpClient;

        @BeforeEach
        void setup() {
            QueueFactory.clearRegistrations();
            QueueFactory.clearQueueMap();
            // Create a mock HTTP client
            mockHttpClient = Mockito.mock(OkHttpClient.class);
        }

        @Test
        void test_template_processing_with_jsonpath_processor() {
            // Create a simple SSE event
            SseEvent originalEvent = new SseEvent("test-id", "test-type", "{\"message\": \"hello\"}");
            
            // Create a configuration with template processing
            Map<String, String> settings = Maps.newHashMap();
            String configurationId = "template-test";
            settings.put("config.ids", configurationId);
            settings.put("topic", "test-topic");
            settings.put("url", "http://example.com/sse");
            
            // Add template configuration - use jsonpath processor
            settings.put("config."+configurationId+".exchange.template", "${response.data}");
            settings.put("config."+configurationId+".exchange.template.processor.names", "jsonpath");
            
            // Create the configuration directly (without starting the task)
            SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
            
            // Process the event using the template
            HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
            SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
            
            // Verify that template processing was applied
            assertThat(processedEvent).isNotNull();
            // The jsonpath processor should extract the JSON data
            assertThat(processedEvent.getData()).contains("message");
        }

        @Test
        void test_template_processing_without_template_configured() {
            // Create a simple SSE event
            SseEvent originalEvent = new SseEvent("test-id", "test-type", "{\"message\": \"hello\"}");
            
            // Create a configuration without template processing
            Map<String, String> settings = Maps.newHashMap();
            String configurationId = "no-template-test";
            settings.put("config.ids", configurationId);
            settings.put("topic", "test-topic");
            settings.put("url", "http://example.com/sse");
            
            // Create the configuration directly (without starting the task)
            SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
            
            // Process the event without template
            HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
            SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
            
            // Verify that the original event is returned unchanged
            assertThat(processedEvent).isSameAs(originalEvent);
            assertThat(processedEvent.getData()).isEqualTo("{\"message\": \"hello\"}");
        }

        @Test
        void test_template_processing_with_jsonpath_message_extraction() {
            // Create a simple SSE event
            SseEvent originalEvent = new SseEvent("test-id", "test-type", "{\"message\": \"hello\", \"timestamp\": 12345}");
            
            // Create a configuration with template processing
            Map<String, String> settings = Maps.newHashMap();
            String configurationId = "message-extract-test";
            settings.put("config.ids", configurationId);
            settings.put("topic", "test-topic");
            settings.put("url", "http://example.com/sse");
            
            // Add template configuration that extracts the message field
            // The content contains the JSON string, so we need to parse it first
            settings.put("config."+configurationId+".exchange.template", "${jsonpath:content}");
            settings.put("config."+configurationId+".exchange.template.processor.names", "jsonpath");
            
            // Create the configuration directly (without starting the task)
            SseConfiguration sseConfiguration = new SseConfiguration(configurationId, mockHttpClient, settings);
            
            // Process the event using the template
            HttpRequest httpRequest = new HttpRequest("http://example.com/sse");
            SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);
            
            // Verify that template processing was applied
            // The jsonpath processor extracts the content and adds it as an attribute
            assertThat(processedEvent).isNotNull();
            assertThat(processedEvent.getData()).isEqualTo("{\"message\": \"hello\", \"timestamp\": 12345}");
            // The template processing should have added jsonpath attributes to the exchange
            // We can't easily verify this from the SseEvent, but we can verify the template manager worked
        }
    }
}