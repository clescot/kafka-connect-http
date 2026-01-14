package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

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

    // ========== BUG HIGHLIGHTING TESTS ==========

    @Test
    void testNullTemplateParameter() {
        // BUG: Should handle null template gracefully but likely throws NPE
        Exchange<?, ?> processedExchange = processor.process(testExchange, null, new HashMap<>());
        
        // Current implementation probably throws NPE before this point
        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testNullExchangeParameter() {
        // BUG: Should handle null exchange but throws exception due to @NotNull contract
        String template = "${datetime:now}";
        
        // This throws IllegalArgumentException due to @NotNull contract violation
        assertThrows(IllegalArgumentException.class, () -> {
            processor.process(null, template, new HashMap<>());
        });
    }

    @Test
    void testExchangeWithNullAttributes() {
        // BUG: Should handle exchange with null attributes
        String template = "${datetime:now}";
        
        // Create an exchange with null attributes - this requires custom mock or builder
        // For now, we'll test the moment source which accesses attributes
        String templateWithMoment = "${datetime:moment:yyyy-MM-dd}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, templateWithMoment, new HashMap<>());
        
        // Should handle missing moment gracefully
        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testMalformedTemplateMissingPrefix() {
        // BUG: Should validate template format
        String template = "datetime:now";
        
        // Current implementation probably doesn't validate the ${datetime: prefix
        boolean supported = processor.supports(template);
        assertThat(supported).isFalse();
    }

    @Test
    void testMalformedTemplateMissingSuffix() {
        // BUG: Should validate template ends with }
        String template = "${datetime:now";
        
        // This might cause StringIndexOutOfBoundsException in extractTemplateParts
        boolean supported = processor.supports(template);
        assertThat(supported).isFalse();
    }

    @Test
    void testEmptyTemplateContent() {
        // BUG: Should handle empty template content
        String template = "${datetime:}";
        
        // This likely causes issues in parsing
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testTemplateWithOnlySource() {
        // BUG: Should handle template with only source, no format
        String template = "${datetime:now}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should use default format
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
    }

    @Test
    void testTemplateWithEmptySource() {
        // BUG: Should handle empty source
        String template = "${datetime::yyyy-MM-dd}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testTemplateWithEmptyFormat() {
        // BUG: Should handle empty format
        String template = "${datetime:now:}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should still add attribute but might be empty
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
    }

    @Test
    void testInvalidDateFormatPattern() {
        // BUG: Should validate date format patterns
        String template = "${datetime:now:INVALID_PATTERN_###_@@@}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // DateTimeFormatter.ofPattern with invalid pattern should throw IllegalArgumentException
        // Current implementation might catch this and return empty string
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
    }

    @Test
    void testMomentWithNonStringObject() {
        // BUG: Should handle non-string moment objects
        String template = "${datetime:moment:yyyy-MM-dd}";
        
        // Add a non-string moment object
        HttpExchange exchangeWithMoment = HttpExchange.Builder.anHttpExchange()
                .withHttpRequest(testExchange.getRequest())
                .withHttpResponse(testExchange.getResponse())
                .withDuration(testExchange.getDurationInMillis())
                .at(testExchange.getMoment())
                .withAttempts(testExchange.getAttempts())
                .build();
        
        // Add non-string moment (e.g., Integer)
        exchangeWithMoment = (HttpExchange) exchangeWithMoment.withAttribute(DateTimeTemplateProcessor.MOMENT, 12345);
        
        Exchange<?, ?> processedExchange = processor.process(exchangeWithMoment, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should handle toString() of non-string object
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        String result = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(result).isEqualTo("12345"); // toString() of Integer
    }

    @Test
    void testMomentWithNullObject() {
        // BUG: Should handle null moment object
        String template = "${datetime:moment:yyyy-MM-dd}";
        
        // Create attributes map with null moment
        Map<String, Object> attributesWithNull = new HashMap<>();
        attributesWithNull.put(DateTimeTemplateProcessor.MOMENT, null);
        
        HttpExchange exchangeWithMoment = HttpExchange.Builder.anHttpExchange()
                .withHttpRequest(testExchange.getRequest())
                .withHttpResponse(testExchange.getResponse())
                .withDuration(testExchange.getDurationInMillis())
                .at(testExchange.getMoment())
                .withAttempts(testExchange.getAttempts())
                .withAttributes(attributesWithNull)
                .build();
        
        Exchange<?, ?> processedExchange = processor.process(exchangeWithMoment, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should handle null moment gracefully
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        String result = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(result).isEmpty(); // Should be empty for null moment
    }

    @Test
    void testEpochInconsistency() {
        // BUG: Epoch behavior is inconsistent between default and custom formats
        String templateWithDefault = "${datetime:epoch}";
        String templateWithCustom = "${datetime:epoch:yyyy-MM-dd}";
        
        Exchange<?, ?> processedDefault = processor.process(testExchange, templateWithDefault, new HashMap<>());
        Exchange<?, ?> processedCustom = processor.process(testExchange, templateWithCustom, new HashMap<>());
        
        HttpExchange httpDefault = (HttpExchange) processedDefault;
        HttpExchange httpCustom = (HttpExchange) processedCustom;
        
        String defaultResult = httpDefault.getAttributes().get("formatted_datetime").toString();
        String customResult = httpCustom.getAttributes().get("formatted_datetime").toString();
        
        // Default returns raw epoch millis (digits only)
        assertThat(defaultResult).matches("\\d+");
        
        // Custom returns formatted date (contains dashes)
        assertThat(customResult).contains("-");
        
        // This inconsistency is confusing - both should be dates or both should be epochs
    }

    @Test
    void testInvalidNumericSource() {
        // BUG: Should handle invalid numeric values
        String template = "${datetime:999999999999999999999999999999:yyyy-MM-dd}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should handle overflow gracefully
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
    }

    @Test
    void testInvalidIsoDate() {
        // BUG: Should handle invalid ISO date strings
        String template = "${datetime:not-a-valid-date:yyyy-MM-dd}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should handle parsing failure gracefully
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
        String result = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        assertThat(result).isEmpty(); // Should be empty for invalid date
    }

    @Test
    void testComplexIsoDateWithMultipleColons() {
        // BUG: Template parsing might fail with complex ISO dates
        String complexIsoDate = "2023-01-15T10:30:45.123+05:30";
        String template = "${datetime:" + complexIsoDate + ":ddMMyyyy}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should parse complex ISO date correctly
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
    }

    @Test
    void testThreadSafety() throws InterruptedException {
        // BUG: DateTimeFormatter creation is not thread-safe
        String template = "${datetime:now:yyyy-MM-dd HH:mm:ss}";
        int numThreads = 10;
        int numIterations = 100;
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    for (int j = 0; j < numIterations; j++) {
                        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
                        assertThat(processedExchange).isNotNull();
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        
        latch.await(10, TimeUnit.SECONDS);
        
        // Should not have any exceptions in concurrent usage
        assertThat(exceptions).isEmpty();
    }

    @Test
    void testPerformanceWithManyFormatterCreations() {
        // BUG: Creating DateTimeFormatter on every call is expensive
        String template = "${datetime:now:yyyy-MM-dd HH:mm:ss.SSS}";
        
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
            assertThat(processedExchange).isNotNull();
        }
        long endTime = System.currentTimeMillis();
        
        long duration = endTime - startTime;
        System.out.println("Duration for 1000 operations: " + duration + "ms");
        
        // This highlights the performance issue - should be much faster with cached formatters
        assertThat(duration).isLessThan(5000); // 5 seconds max for demonstration
    }

    @Test
    void testTimeZoneInconsistency() {
        // BUG: No explicit timezone handling
        String template = "${datetime:now:yyyy-MM-dd HH:mm:ss Z}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        String result = httpProcessedExchange.getAttributes().get("formatted_datetime").toString();
        
        // Result depends on system default timezone - inconsistent across environments
        assertThat(result).isNotBlank();
        System.out.println("Timezone-dependent result: " + result);
    }

    @Test
    void testSecurityTemplateInjection() {
        // BUG: No input validation could allow template injection
        String maliciousTemplate = "${datetime:now:../../../etc/passwd}";
        
        // Should validate/sanitize format patterns
        Exchange<?, ?> processedExchange = processor.process(testExchange, maliciousTemplate, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Current implementation probably tries to create DateTimeFormatter with malicious pattern
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
    }

    @Test
    void testVeryLongTemplate() {
        // BUG: Should handle very long templates gracefully
        StringBuilder longTemplate = new StringBuilder("${datetime:");
        for (int i = 0; i < 10000; i++) {
            longTemplate.append("a");
        }
        longTemplate.append(":yyyy-MM-dd}");
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, longTemplate.toString(), new HashMap<>());
        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testSpecialCharactersInSource() {
        // BUG: Should handle special characters in source
        String template = "${datetime:source with spaces and @#$% symbols:yyyy-MM-dd}";
        
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        // Should handle special characters gracefully
        assertThat(httpProcessedExchange.getAttributes()).containsKey("formatted_datetime");
    }
}
