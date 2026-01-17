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

class RandomExchangeTemplateProcessorTest {

    private RandomExchangeTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new RandomExchangeTemplateProcessor();

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
        assertThat(processor.getName()).isEqualTo("random");
    }

    @Test
    void testSupportsValidTemplates() {
        assertThat(processor.supports("${random.int}")).isTrue();
        assertThat(processor.supports("${random.int:0:100}")).isTrue();
        assertThat(processor.supports("${random.long:1000:9999}")).isTrue();
        assertThat(processor.supports("${random.double}")).isTrue();
        assertThat(processor.supports("${random.uuid}")).isTrue();
        assertThat(processor.supports("${random.string:20}")).isTrue();
        assertThat(processor.supports("${random.boolean}")).isTrue();
        assertThat(processor.supports("${random.str:15}")).isTrue();
    }

    @Test
    void testSupportsInvalidTemplates() {
        assertThat(processor.supports("${jsonpath:$.test}")).isFalse();
        assertThat(processor.supports("${random:}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports("${random")).isFalse();
        assertThat(processor.supports("random.int}")).isFalse();
        assertThat(processor.supports(null)).isFalse();
        assertThat(processor.supports("${random.}")).isFalse();
    }

    @Test
    void testProcessWithRandomInt() {
        String template = "${random.int:0:100}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).isNotEmpty();
        int value = Integer.parseInt(content);
        assertThat(value).isBetween(0, 100);
    }

    @Test
    void testProcessWithRandomLong() {
        String template = "${random.long:1000000:9999999}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).isNotEmpty();
        long value = Long.parseLong(content);
        assertThat(value).isBetween(1000000L, 9999999L);
    }

    @Test
    void testProcessWithRandomDouble() {
        String template = "${random.double:10:20}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).isNotEmpty();
        double value = Double.parseDouble(content);
        assertThat(value).isBetween(10.0, 20.0);
    }

    @Test
    void testProcessWithRandomUUID() {
        String template = "${random.uuid}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    void testProcessWithRandomString() {
        String template = "${random.string:15}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).hasSize(15);
        assertThat(content).matches("[A-Za-z0-9]+");
    }

    @Test
    void testProcessWithRandomStringDefaultLength() {
        String template = "${random.string}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).hasSize(10);
        assertThat(content).matches("[A-Za-z0-9]+");
    }

    @Test
    void testProcessWithRandomBoolean() {
        String template = "${random.boolean}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).isIn("true", "false");
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("existing", "value");

        String template = "${random.int:0:100}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("existing");
        assertThat(httpProcessedExchange.getAttributes().get("existing").toString()).isEqualTo("value");
    }

    @Test
    void testProcessWithUnknownType() {
        String template = "${random.unknown_type}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        assertThat(processedExchange).isEqualTo(testExchange);
    }

    @Test
    void testProcessWithIntegerAlias() {
        String template = "${random.integer:0:50}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).isNotEmpty();
        int value = Integer.parseInt(content);
        assertThat(value).isBetween(0, 50);
    }

    @Test
    void testProcessWithBoolAlias() {
        String template = "${random.bool}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).isIn("true", "false");
    }

    @Test
    void testProcessWithFloatAlias() {
        String template = "${random.float:0:10}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String content = httpProcessedExchange.getContent();
        assertThat(content).isNotEmpty();
        double value = Double.parseDouble(content);
        assertThat(value).isBetween(0.0, 10.0);
    }

    @Test
    void testProcessWithInvalidNumberFormat() {
        String template = "${random.int:abc:100}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        assertThat(processedExchange).isEqualTo(testExchange);
    }
}
