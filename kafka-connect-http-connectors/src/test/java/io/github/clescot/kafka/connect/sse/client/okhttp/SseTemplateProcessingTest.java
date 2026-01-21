package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Maps;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.client.queue.QueueFactory;
import io.github.clescot.core.sse.SseEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class SseTemplateProcessingTest {

    @Nested
    class TemplateProcessing {
        SseSourceTask sseSourceTask;

        @BeforeEach
        void setup() {
            QueueFactory.clearRegistrations();
            QueueFactory.clearQueueMap();
            sseSourceTask = new SseSourceTask();
        }

        @AfterEach
        void shutdown() {
            sseSourceTask.stop();
        }

        @Test
        void test_template_processing_with_simple_modification() {
            // Create a simple SSE event
            SseEvent originalEvent = new SseEvent("test-id", "test-type", "{\"message\": \"hello\"}");

            // Create a configuration with template processing
            Map<String, String> settings = Maps.newHashMap();
            String configurationId = "template-test";
            settings.put("config.ids", configurationId);
            settings.put("config." + configurationId + ".topic", "test-topic");
            settings.put("config." + configurationId + ".url", "http://localhost:8080/sse");

            // Add template configuration - use correct JSONPath syntax
            settings.put("config." + configurationId + ".exchange.template", "${jsonpath:content}");
            settings.put("config." + configurationId + ".exchange.template.processor.names", "jsonpath");

            sseSourceTask.start(settings);

            // Get the configuration and queue
            Map<String, SseConfiguration> configurations = sseSourceTask.getConfigurations();
            SseConfiguration sseConfiguration = configurations.get(configurationId);
            Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId).orElseThrow();

            // Add the event to the queue
            queue.add(originalEvent);

            // Process the event using the template
            HttpRequest httpRequest = new HttpRequest("http://localhost:8080/sse");
            SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);

            // Verify that template processing was applied
            assertThat(processedEvent).isNotNull();
            // The jsonpath processor extracts the content and adds it as an attribute
            // The original data should remain unchanged
            assertThat(processedEvent.getData()).isEqualTo("{\"message\": \"hello\"}");
        }

        @Test
        void test_template_processing_without_template_configured() {
            // Create a simple SSE event
            SseEvent originalEvent = new SseEvent("test-id", "test-type", "{\"message\": \"hello\"}");

            // Create a configuration without template processing
            Map<String, String> settings = Maps.newHashMap();
            String configurationId = "no-template-test";
            settings.put("config.ids", configurationId);
            settings.put("config." + configurationId + ".topic", "test-topic");
            settings.put("config." + configurationId + ".url", "http://localhost:8080/sse");

            sseSourceTask.start(settings);

            // Get the configuration and queue
            Map<String, SseConfiguration> configurations = sseSourceTask.getConfigurations();
            SseConfiguration sseConfiguration = configurations.get(configurationId);

            // Process the event without template
            HttpRequest httpRequest = new HttpRequest("http://localhost:8080/sse");
            SseEvent processedEvent = sseConfiguration.processEventWithTemplate(originalEvent, httpRequest);

            // Verify that the original event is returned unchanged
            assertThat(processedEvent).isSameAs(originalEvent);
            assertThat(processedEvent.getData()).isEqualTo("{\"message\": \"hello\"}");
        }

        @Test
        void test_poll_with_template_processing() {
            // Create a simple SSE event
            SseEvent originalEvent = new SseEvent("test-id", "test-type", "{\"message\": \"hello\"}");

            // Create a configuration with template processing
            Map<String, String> settings = Maps.newHashMap();
            String configurationId = "poll-template-test";
            settings.put("config.ids", configurationId);
            settings.put("config." + configurationId + ".topic", "test-topic");
            settings.put("config." + configurationId + ".url", "http://localhost:8080/sse");

            // Add template configuration that extracts the message using correct JSONPath
            // syntax
            // The content contains the JSON string, so we need to parse it first
            settings.put("config." + configurationId + ".exchange.template", "${jsonpath:content}");
            settings.put("config." + configurationId + ".exchange.template.processor.names", "jsonpath");

            sseSourceTask.start(settings);

            // Get the queue and add the event
            Queue<SseEvent> queue = sseSourceTask.getQueue(configurationId).orElseThrow();
            queue.add(originalEvent);

            // Poll to process the event
            List<org.apache.kafka.connect.source.SourceRecord> records = sseSourceTask.poll();

            // Verify that the record was created with the processed data
            assertThat(records).hasSize(1);
            org.apache.kafka.connect.source.SourceRecord record = records.get(0);

            // The data should be the processed content (JSONPath result) with empty attributes
            // ${jsonpath:content} extracts the content, so data stays the same
            assertThat(record.value()).isEqualTo(
                    "{\"id\":\"test-id\",\"attributes\":\"{}\",\"type\":\"test-type\",\"data\":\"{\"message\": \"hello\"}\"}");
        }
    }
}