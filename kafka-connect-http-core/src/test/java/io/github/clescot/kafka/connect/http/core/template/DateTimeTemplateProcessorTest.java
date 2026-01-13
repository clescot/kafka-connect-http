package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DateTimeTemplateProcessorTest {

    private DateTimeTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new DateTimeTemplateProcessor();
        
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
    void testGetName() {
        assertThat(processor.getName()).isEqualTo("datetime");
    }

    @Test
    void testSupportsValidTemplate() {
        assertTrue(processor.supports("${datetime:now}"));
        assertTrue(processor.supports("${datetime:now:yyyy-MM-dd}"));
        assertTrue(processor.supports("${datetime:current:HH:mm:ss:custom_attr}"));
    }

    @Test
    void testSupportsInvalidTemplate() {
        assertFalse(processor.supports("${jsonpath:$.test}"));
        assertFalse(processor.supports("plain text"));
        assertFalse(processor.supports("${datetime}")); // Missing colon
        assertFalse(processor.supports("datetime:now}")); // Missing ${ prefix
    }

    @Test
    void testProcessWithNowSourceAndDefaultFormat() {
        String template = "${datetime:now}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Verify the value is a valid date string
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).isNotBlank();
    }

    @Test
    void testProcessWithCurrentSourceAndCustomFormat() {
        String template = "${datetime:current:yyyy-MM-dd}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Verify the value matches the expected format (yyyy-MM-dd)
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).matches("\\d{4}-\\d{2}-\\d{2}");
    }

    @Test
    void testProcessWithCustomAttributeName() {
        String template = "${datetime:now:yyyy-MM-dd:custom_date}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("custom_date");
        
        // Verify the value matches the expected format (yyyy-MM-dd)
        String dateTimeValue = httpProcessedExchange.getAttributes().get("custom_date").toString();
        assertThat(dateTimeValue).matches("\\d{4}-\\d{2}-\\d{2}");
    }

    @Test
    void testProcessWithEpochSource() {
        String template = "${datetime:epoch}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Verify the value is a numeric epoch timestamp
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).matches("\\d+");
    }

    @Test
    void testProcessWithSpecificEpochTimestamp() {
        long testEpoch = System.currentTimeMillis();
        String template = "${datetime:" + testEpoch + ":yyyyMMdd}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Verify the value matches the expected format
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).matches("\\d{8}");
    }

    @Test
    void testProcessWithIsoDateString() {
        // Using a proper ISO date format
        String isoDate = "2023-01-15T10:30:45Z";
        String template = "${datetime:" + isoDate + ":ddMMyyyy}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Verify the value is formatted as ddMMyyyy
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).isEqualTo("15012023");
    }

    @Test
    void testProcessWithMomentSourceAndValidMetadata() {
        // Add moment metadata to the exchange
        ZonedDateTime moment = ZonedDateTime.now();
        Map<String, Object> metadata = new HashMap<>(testExchange.getMetadata());
        metadata.put(DateTimeTemplateProcessor.MOMENT, moment.toString());
        
        // Create a new exchange with moment metadata
        HttpExchange exchangeWithMoment = HttpExchange.Builder.anHttpExchange()
                .withHttpRequest(testExchange.getRequest())
                .withHttpResponse(testExchange.getResponse())
                .withDuration(testExchange.getDurationInMillis())
                .at(testExchange.getMoment())
                .withAttempts(testExchange.getAttempts())
                .build();
        
        // Add the moment metadata
        exchangeWithMoment = (HttpExchange) exchangeWithMoment.withAttribute(DateTimeTemplateProcessor.MOMENT, moment.toString());
        
        String template = "${datetime:moment:yyyyMMdd}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithMoment, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Verify the value matches the expected format
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).matches("\\d{8}");
    }

    @Test
    void testProcessWithMomentSourceAndMissingMetadata() {
        String template = "${datetime:moment:yyyyMMdd}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Should be empty since moment metadata is missing
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).isEmpty();
    }
    
    @Test
    void testProcessWithInvalidSource() {
        String template = "${datetime:invalid_source}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        
        // Should be empty for invalid source
        String dateTimeValue = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(dateTimeValue).isEmpty();
    }
    
    @Test
    void testProcessWithInvalidDateFormat() {
        String template = "${datetime:now:invalid_format}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should still add the attribute, but might be empty or contain error info
        assertThat(httpProcessedExchange.getAttributes()).isNotEmpty();
    }
}
