package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderParameterTemplateProcessorTest {

    private HttpExchange testExchange;
    private HeaderParameterTemplateProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new HeaderParameterTemplateProcessor();

        HttpRequest request = new HttpRequest("http://example.com/api/test?param1=value1&param2=value2");
        request.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        request.getHeaders().put("Authorization", Collections.singletonList("Bearer token123"));
        request.getHeaders().put("Cookie", Collections.singletonList("sessionId=abc123; userId=456"));
        request.setBodyAsString("{\"input\": \"test data\"}");

        HttpResponse response = new HttpResponse(200, "OK");
        response.getHeaders().put("X-Custom-Header", Collections.singletonList("custom-value"));
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
    void testSupportsHeaderTemplate() {
        assertThat(processor.supports("${header:Content-Type}")).isTrue();
        assertThat(processor.supports("${header:Authorization}")).isTrue();
    }

    @Test
    void testSupportsParamTemplate() {
        assertThat(processor.supports("${param:param1}")).isTrue();
        assertThat(processor.supports("${param:userId}")).isTrue();
    }

    @Test
    void testSupportsCookieTemplate() {
        assertThat(processor.supports("${cookie:sessionId}")).isTrue();
        assertThat(processor.supports("${cookie:userId}")).isTrue();
    }

    @Test
    void testDoesNotSupportInvalidTemplates() {
        assertThat(processor.supports("${invalid:template}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports("${jsonpath:$.field}")).isFalse();
    }

    @Test
    void testGetName() {
        assertThat(processor.getName()).isEqualTo("header");
    }

    @Test
    void testProcessHeaderFromRequest() {
        String template = "${header:Content-Type}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEqualTo("application/json");
    }

    @Test
    void testProcessHeaderFromResponse() {
        String template = "${header:X-Custom-Header}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEqualTo("custom-value");
    }

    @Test
    void testProcessNonExistentHeader() {
        String template = "${header:Non-Existent-Header}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEmpty();
    }

    @Test
    void testProcessQueryParameter() {
        String template = "${param:param1}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEqualTo("value1");
    }

    @Test
    void testProcessNonExistentQueryParameter() {
        String template = "${param:nonExistent}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEmpty();
    }

    @Test
    void testProcessCookie() {
        String template = "${cookie:sessionId}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEqualTo("abc123");
    }

    @Test
    void testProcessNonExistentCookie() {
        String template = "${cookie:nonExistentCookie}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEmpty();
    }

    @Test
    void testProcessWithNonHttpExchange() {
        Exchange<Request, Response> nonHttpExchange = new Exchange<Request, Response>() {
            @Override
            public Request getRequest() {
                return null;
            }

            @Override
            public Response getResponse() {
                return null;
            }

            @Override
            public Map<String, Object> getAttributes() {
                return new HashMap<>();
            }

            @Override
            public Object getAttribute(String name) {
                return null;
            }

            @Override
            public Exchange<Request, Response> withAttribute(String name, Object value) {
                return this;
            }

            @Override
            public String getContent() {
                return "";
            }

            @Override
            public Exchange<Request, Response> setContent(String content) {
                return null;
            }

            @Override
            public Map<String, Object> getMetadata() {
                return new HashMap<>();
            }
        };

        String template = "${header:Content-Type}";
        Exchange result = processor.process(nonHttpExchange, template, new HashMap<>());

        assertThat(result).isSameAs(nonHttpExchange);
    }

    @Test
    void testProcessWithUnknownType() {
        String template = "${unknown:type}";
        Exchange result = processor.process(testExchange, template, new HashMap<>());

        assertThat(result).isSameAs(testExchange);
    }

    @Test
    void testProcessWithNullRequest() {
        HttpExchange exchangeWithNullRequest = new HttpExchange(
                null,
                testExchange.getResponse(),
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        String template = "${header:Content-Type}";
        Exchange result = processor.process(exchangeWithNullRequest, template, new HashMap<>());

        assertThat(result).isNotNull();
    }

    @Test
    void testProcessWithNullResponse() {
        HttpRequest request = new HttpRequest("http://example.com/api/test");
        request.getHeaders().put("Content-Type", Collections.singletonList("application/json"));

        HttpExchange exchangeWithNullResponse = new HttpExchange(
                request,
                null,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        String template = "${header:Content-Type}";
        Exchange result = processor.process(exchangeWithNullResponse, template, new HashMap<>());

        assertThat(result).isNotNull();
    }

    @Test
    void testProcessWithRequestWithoutQueryParameters() {
        HttpRequest request = new HttpRequest("http://example.com/api/test");
        HttpExchange exchange = new HttpExchange(
                request,
                testExchange.getResponse(),
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        String template = "${param:param1}";
        Exchange result = processor.process(exchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEmpty();
    }

    @Test
    void testProcessWithRequestWithoutCookies() {
        HttpRequest request = new HttpRequest("http://example.com/api/test");
        HttpExchange exchange = new HttpExchange(
                request,
                testExchange.getResponse(),
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        String template = "${cookie:sessionId}";
        Exchange result = processor.process(exchange, template, new HashMap<>());

        assertThat(result).isNotNull();
        HttpExchange httpResult = (HttpExchange) result;
        assertThat(httpResult.getContent()).isEmpty();
    }
}
