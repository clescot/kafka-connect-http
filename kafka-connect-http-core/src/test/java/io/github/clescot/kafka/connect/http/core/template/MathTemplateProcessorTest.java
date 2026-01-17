package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MathTemplateProcessorTest {

    private MathTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new MathTemplateProcessor();

        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        HttpResponse response = new HttpResponse(200, "OK");
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
        assertThat(processor.getName()).isEqualTo("math");
    }

    @Test
    void testSupportsValidTemplates() {
        assertThat(processor.supports("${math:1+1}")).isTrue();
        assertThat(processor.supports("${math:2*3}")).isTrue();
        assertThat(processor.supports("${math:(1+2)*3}")).isTrue();
        assertThat(processor.supports("${math:10/2}")).isTrue();
    }

    @Test
    void testSupportsInvalidTemplates() {
        assertThat(processor.supports("${jsonpath:$.test}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports("${math:1+1")).isFalse();
        assertThat(processor.supports("math:1+1}")).isFalse();
    }

    @Test
    void testProcessWithSimpleAddition() {
        String template = "${math:2+3}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;

        assertThat(httpProcessedExchange.getContent()).isEqualTo("5.0");
    }

    @Test
    void testProcessWithSimpleSubtraction() {
        String template = "${math:10-4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("6.0");
    }

    @Test
    void testProcessWithMultiplication() {
        String template = "${math:6*7}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("42.0");
    }

    @Test
    void testProcessWithDivision() {
        String template = "${math:20/4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("5.0");
    }

    @Test
    void testProcessWithDecimalResult() {
        String template = "${math:10/3}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String result = httpProcessedExchange.getContent();
        assertThat(Double.parseDouble(result)).isCloseTo(3.333, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    void testProcessWithParentheses() {
        String template = "${math:(2+3)*4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("20.0");
    }

    @Test
    void testProcessWithNestedParentheses() {
        String template = "${math:((1+2)*(3+4))}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("21.0");
    }

    @Test
    void testProcessWithMixedOperations() {
        String template = "${math:2+3*4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("14.0");
    }

    @Test
    void testProcessWithNegativeNumbers() {
        String template = "${math:10-5}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("5.0");
    }

    @Test
    void testProcessWithLeadingNegativeResult() {
        String template = "${math:5-10}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("-5.0");
    }

    @Test
    void testProcessWithDecimalNumbers() {
        String template = "${math:2.5*4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("10.0");
    }
}
