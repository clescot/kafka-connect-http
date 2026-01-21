package io.github.clescot.core.template;

import io.github.clescot.core.http.template.RegexExchangeTemplateProcessor;
import io.github.clescot.core.http.Exchange;
import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RegexExchangeTemplateProcessorTest {

    private RegexExchangeTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new RegexExchangeTemplateProcessor();

        String content = "Order ID: 12345, Customer: John Doe, Amount: $99.99";

        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);

        HttpResponse response = new HttpResponse(200, "OK");
        response.setBodyAsString(content);

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
        assertThat(processor.getName()).isEqualTo("regex");
    }

    @Test
    void testSupportsValidTemplates() {
        assertThat(processor.supports("${regex:\\d+}")).isTrue();
        assertThat(processor.supports("${regex:[a-z]+}")).isTrue();
        assertThat(processor.supports("${regex:Order ID: (\\d+)}")).isTrue();
        assertThat(processor.supports("${regex:pattern}")).isTrue();
    }

    @Test
    void testSupportsInvalidTemplates() {
        assertThat(processor.supports("${jsonpath:$.test}")).isFalse();
        assertThat(processor.supports("${regex}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports("${regex")).isFalse();
        assertThat(processor.supports("regex:pattern}")).isFalse();
    }

    @Test
    void testProcessWithSimpleRegex() {
        String template = "${regex:\\d+}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isNotEmpty();
    }

    @Test
    void testProcessWithCaptureGroup() {
        String template = "${regex:Order ID: (\\d+)}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("12345");
    }

    @Test
    void testProcessWithSecondCaptureGroup() {
        String template = "${regex:Customer: ([A-Za-z ]+)}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("John Doe");
    }

    @Test
    void testProcessWithNoMatch() {
        String template = "${regex:NotFound\\d+}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("existing", "value");

        String template = "${regex:\\d+}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("existing");
        assertThat(httpProcessedExchange.getAttributes().get("existing").toString()).isEqualTo("value");
    }

    @Test
    void testProcessWithInvalidRegex() {
        String template = "${regex:[invalid(result}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        assertThat(processedExchange).isEqualTo(testExchange);
    }

    @Test
    void testProcessWithEmptyContent() {
        HttpRequest emptyRequest = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        emptyRequest.setBodyAsString("");

        HttpResponse response = new HttpResponse(200, "OK");

        HttpExchange emptyExchange = new HttpExchange(
                emptyRequest,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        String template = "${regex:\\d+}";
        Exchange<?, ?> processedExchange = processor.process(emptyExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessWithEmailPattern() {
        String template = "${regex:[a-zA-Z]+@[a-zA-Z]+\\.[a-z]+}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessWithWhitespaceRegex() {
        String template = "${regex:\\s+}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isNotEmpty();
    }

    @Test
    void testProcessWithAlternation() {
        String template = "${regex:(Jane|George)}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessWithMultipleMatches() {
        String template = "${regex:\\d+}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isNotEmpty();
    }

    @Test
    void testProcessWithComplexPattern() {
        String template = "${regex:\\$([0-9]+\\.[0-9]{2})}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("99.99");
    }
}
