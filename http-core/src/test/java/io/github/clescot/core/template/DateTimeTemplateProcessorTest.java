package io.github.clescot.core.template;

import io.github.clescot.core.http.template.DateTimeTemplateProcessor;
import io.github.clescot.core.http.Exchange;
import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class DateTimeTemplateProcessorTest {

    private DateTimeTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new DateTimeTemplateProcessor();

        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        request.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        request.setBodyAsString("{\"input\": \"test data\"}");

        HttpResponse response = new HttpResponse(200, "OK");
        response.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        response.setBodyAsString("original content");

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
    }

    @Test
    void testSupportsInvalidTemplate() {
        assertFalse(processor.supports("${jsonpath:$.test}"));
        assertFalse(processor.supports("plain text"));
        assertFalse(processor.supports("${datetime}"));
        assertFalse(processor.supports("datetime:now}"));
    }

    @Test
    void testProcessWithNowSourceAndDefaultFormat() {
        String template = "${datetime:now}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;

        String dateTimeValue = httpProcessedExchange.getContent();
        assertThat(dateTimeValue).isNotBlank();
    }

    @Test
    void testProcessWithCurrentSourceAndCustomFormat() {
        String template = "${datetime:current:yyyy-MM-dd}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;

        String dateTimeValue = httpProcessedExchange.getContent();
        assertThat(dateTimeValue).matches("\\d{4}-\\d{2}-\\d{2}");
    }

    @Test
    void testProcessWithEpochSource() {
        String template = "${datetime:epoch}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;

        String dateTimeValue = httpProcessedExchange.getContent();
        assertThat(dateTimeValue).matches("\\d+");
    }

    @Test
    void testProcessWithSpecificEpochTimestamp() {
        long testEpoch = System.currentTimeMillis();
        String template = "${datetime:" + testEpoch + ":yyyyMMdd}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;

        String dateTimeValue = httpProcessedExchange.getContent();
        assertThat(dateTimeValue).matches("\\d{8}");
    }

    @Test
    void testProcessWithInvalidSource() {
        String template = "${datetime:invalid_source}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEmpty();
    }

    @Test
    void testTemplateWithOnlySource() {
        String template = "${datetime:now}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isNotEmpty();
    }

    @Test
    void testVeryLongTemplate() {
        String template = "${datetime:now:" + "a".repeat(500) + "}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithInvalidDateFormatPattern() {
        String template = "${datetime:now:invalid_pattern}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isNotNull();
    }

    @Test
    void testNullTemplateParameter() {
        Exchange<?, ?> processedExchange = processor.process(testExchange, null, new HashMap<>());

        assertThat(processedExchange).isEqualTo(testExchange);
    }

    @Test
    void testNullExchangeParameter() {
        assertThrows(IllegalArgumentException.class, () -> {
            processor.process(null, "${datetime:now}", new HashMap<>());
        });
    }
}
