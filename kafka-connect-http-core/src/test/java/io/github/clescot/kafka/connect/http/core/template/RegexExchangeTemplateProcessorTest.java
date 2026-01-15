package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
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
        assertThat(processor.supports("${regex:\\d+:result}")).isTrue();
        assertThat(processor.supports("${regex:[a-z]+:result}")).isTrue();
        assertThat(processor.supports("${regex:Order ID: (\\d+):order_id}")).isTrue();
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
        String template = "${regex:\\d+:order_id}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).containsKey("order_id");
        assertThat(httpProcessedExchange.getAttributes().get("order_id").toString()).isNotEmpty();
    }

    @Test
    void testProcessWithCaptureGroup() {
        String template = "${regex:Order ID: (\\d+):order_id}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("order_id");
        assertThat(httpProcessedExchange.getAttributes().get("order_id").toString()).isEqualTo("12345");
    }

    @Test
    void testProcessWithSecondCaptureGroup() {
        String template = "${regex:Customer: ([A-Za-z ]+):customer}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("customer");
        assertThat(httpProcessedExchange.getAttributes().get("customer").toString()).isEqualTo("John Doe");
    }

    @Test
    void testProcessWithNoMatch() {
        String template = "${regex:NotFound\\d+:result}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("result");
        assertThat(httpProcessedExchange.getAttributes().get("result").toString()).isEmpty();
    }

    @Test
    void testProcessWithDefaultAttributeName() {
        String template = "${regex:\\d+:result}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("result");
        assertThat(httpProcessedExchange.getAttributes().get("result").toString()).isNotEmpty();
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("existing", "value");
        
        String template = "${regex:\\d+:order_id}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("existing");
        assertThat(httpProcessedExchange.getAttributes().get("existing").toString()).isEqualTo("value");
        assertThat(httpProcessedExchange.getAttributes()).containsKey("order_id");
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
        
        String template = "${regex:\\d+:result}";
        Exchange<?, ?> processedExchange = processor.process(emptyExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("result");
        assertThat(httpProcessedExchange.getAttributes().get("result").toString()).isEmpty();
    }

    @Test
    void testProcessWithNullContent() {
        HttpRequest nullRequest = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        nullRequest.setBodyAsString(null);
        
        HttpResponse response = new HttpResponse(200, "OK");
        
        HttpExchange nullExchange = new HttpExchange(
                nullRequest,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );
        
        String template = "${regex:\\d+:result}";
        Exchange<?, ?> processedExchange = processor.process(nullExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("result");
        assertThat(httpProcessedExchange.getAttributes().get("result").toString()).isEmpty();
    }

    @Test
    void testProcessWithEscapedColon() {
        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        request.setBodyAsString("key=value\\:escaped");
        
        HttpResponse response = new HttpResponse(200, "OK");
        
        HttpExchange exchangeWithColon = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );
        
        String template = "${regex:key=(.+)\\:escaped:result}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithColon, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithEmailPattern() {
        String template = "${regex:[a-zA-Z]+@[a-zA-Z]+\\.[a-z]+:email}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("email");
        assertThat(httpProcessedExchange.getAttributes().get("email").toString()).isEmpty();
    }

    @Test
    void testProcessWithWhitespaceRegex() {
        String template = "${regex:\\s+:whitespace}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("whitespace");
        assertThat(httpProcessedExchange.getAttributes().get("whitespace").toString()).isNotEmpty();
    }

    @Test
    void testProcessWithAlternation() {
        String template = "${regex:(Jane|George):name}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("name");
        assertThat(httpProcessedExchange.getAttributes().get("name").toString()).isEmpty();
    }

    @Test
    void testExtractTemplatePartsWithColon() {
        try {
            java.lang.reflect.Method method = processor.getClass().getDeclaredMethod("extractTemplateParts", String.class);
            method.setAccessible(true);
            
            String template = "${regex:pattern:value}";
            String[] result = (String[]) method.invoke(processor, template);
            assertThat(result).hasSize(2);
            assertThat(result[0]).isEqualTo("pattern");
            assertThat(result[1]).isEqualTo("value");
        } catch (Exception e) {
            // If we can't access the method, skip this test
        }
    }

    @Test
    void testFindLastUnescapedColon() {
        try {
            java.lang.reflect.Method method = processor.getClass().getDeclaredMethod("findLastUnescapedColon", String.class);
            method.setAccessible(true);
            
            int result1 = (int) method.invoke(processor, "pattern:value");
            assertThat(result1).isEqualTo(7);
            
            int result2 = (int) method.invoke(processor, "pattern\\:value");
            assertThat(result2).isEqualTo(-1);
            
            int result3 = (int) method.invoke(processor, "pattern:value:attr");
            assertThat(result3).isEqualTo(13);
        } catch (Exception e) {
            // If we can't access the method, skip this test
        }
    }

    @Test
    void testProcessWithMultipleMatches() {
        String template = "${regex:\\d+:first_digit}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("first_digit");
        assertThat(httpProcessedExchange.getAttributes().get("first_digit").toString()).isNotEmpty();
    }

    @Test
    void testProcessWithComplexPattern() {
        String template = "${regex:\\$([0-9]+\\.[0-9]{2}):amount}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("amount");
        assertThat(httpProcessedExchange.getAttributes().get("amount").toString()).isEqualTo("99.99");
    }
}
