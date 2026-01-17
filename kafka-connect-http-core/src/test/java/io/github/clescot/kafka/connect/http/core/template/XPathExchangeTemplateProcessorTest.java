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

class XPathExchangeTemplateProcessorTest {

    private XPathExchangeTemplateProcessor processor;
    private HttpExchange testExchangeWithXml;
    private HttpExchange testExchangeWithJson;

    @BeforeEach
    void setUp() {
        processor = new XPathExchangeTemplateProcessor();

        String xmlContent = "<root><name>John</name><age>30</age></root>";

        HttpRequest xmlRequest = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        xmlRequest.setBodyAsString(xmlContent);

        HttpResponse xmlResponse = new HttpResponse(200, "OK");
        xmlResponse.setBodyAsString(xmlContent);

        testExchangeWithXml = new HttpExchange(
                xmlRequest,
                xmlResponse,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        String jsonContent = "{\"name\": \"John\", \"age\": 30}";

        HttpRequest jsonRequest = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        jsonRequest.setBodyAsString(jsonContent);

        HttpResponse jsonResponse = new HttpResponse(200, "OK");
        jsonResponse.setBodyAsString(jsonContent);

        testExchangeWithJson = new HttpExchange(
                jsonRequest,
                jsonResponse,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );
    }

    @Test
    void testGetName() {
        assertThat(processor.getName()).isEqualTo("xpath");
    }

    @Test
    void testSupportsValidTemplates() {
        assertThat(processor.supports("${xpath:/root/name}")).isTrue();
        assertThat(processor.supports("${xpath://name}")).isTrue();
        assertThat(processor.supports("${xpath:/root/*}")).isTrue();
    }

    @Test
    void testSupportsInvalidTemplates() {
        assertThat(processor.supports("${jsonpath:$.test}")).isFalse();
        assertThat(processor.supports("${xpath}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports("${xpath")).isFalse();
        assertThat(processor.supports("xpath:/root}")).isFalse();
        assertThat(processor.supports(null)).isFalse();
    }

    @Test
    void testProcessWithSimpleXPath() {
        String template = "${xpath:/root/name}";
        Exchange<?, ?> processedExchange = processor.process(testExchangeWithXml, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("John");
    }

    @Test
    void testProcessWithDifferentXPath() {
        String template = "${xpath:/root/age}";
        Exchange<?, ?> processedExchange = processor.process(testExchangeWithXml, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEqualTo("30");
    }

    @Test
    void testProcessWithNonXmlContent() {
        String template = "${xpath:/root/name}";
        Exchange<?, ?> processedExchange = processor.process(testExchangeWithJson, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchangeWithXml.withAttribute("existing", "value");

        String template = "${xpath:/root/name}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("existing");
        assertThat(httpProcessedExchange.getAttributes().get("existing").toString()).isEqualTo("value");
    }

    @Test
    void testProcessWithComplexXPath() {
        String template = "${xpath://*[name='John']}";
        Exchange<?, ?> processedExchange = processor.process(testExchangeWithXml, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithInvalidXPath() {
        String template = "${xpath:/[invalid";
        Exchange<?, ?> processedExchange = processor.process(testExchangeWithXml, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        assertThat(processedExchange).isEqualTo(testExchangeWithXml);
    }

    @Test
    void testProcessWithWildcardXPath() {
        String template = "${xpath:/root/*}";
        Exchange<?, ?> processedExchange = processor.process(testExchangeWithXml, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithEmptyXmlContent() {
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

        String template = "${xpath:/root/name}";
        Exchange<?, ?> processedExchange = processor.process(emptyExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
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

        String template = "${xpath:/root/name}";
        Exchange<?, ?> processedExchange = processor.process(nullExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithMultipleXPathExpressions() {
        String template = "${xpath:/root/name}";
        Exchange<?, ?> processedExchange = processor.process(testExchangeWithXml, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isNotEmpty();
    }
}
