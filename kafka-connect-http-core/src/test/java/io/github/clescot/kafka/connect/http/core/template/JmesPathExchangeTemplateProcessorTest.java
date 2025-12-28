package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class JmesPathExchangeTemplateProcessorTest {
    
    private JmesPathExchangeTemplateProcessor processor;
    private HttpExchange exchange;
    
    @BeforeEach
    void setUp() {
        processor = new JmesPathExchangeTemplateProcessor();
        
        // Create a sample HTTP exchange with JSON content
        HttpRequest request = new HttpRequest("http://example.com/api", HttpRequest.Method.GET);
        HttpResponse response = new HttpResponse(200, "OK");
        response.setBodyAsString("{\"user\":{\"id\":123,\"name\":\"John Doe\",\"email\":\"john@example.com\"},\"status\":\"active\",\"timestamp\":1234567890}");
        
        exchange = new HttpExchange(
                request,
                response,
                100,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
    }
    
    @Test
    void testJmesPathProcessorName() {
        assertThat(processor.getName()).isEqualTo("jmespath");
    }
    
    @Test
    void testJmesPathProcessorSupports() {
        assertThat(processor.supports("${jmespath:user.name:result}")).isTrue();
        assertThat(processor.supports("${jmespath:status}")).isTrue();
        assertThat(processor.supports("${jsonpath:user.name}")).isFalse();
        assertThat(processor.supports("${random:uuid}")).isFalse();
    }
    
    @Test
    void testJmesPathExpressionWithAttributeName() {
        Exchange<?, ?> result = processor.process(exchange, "${jmespath:user.name:username}", Collections.emptyMap());
        assertThat(result.getAttribute("username")).isEqualTo("John Doe");
    }
    
    @Test
    void testJmesPathExpressionWithoutAttributeName() {
        Exchange<?, ?> result = processor.process(exchange, "${jmespath:status}", Collections.emptyMap());
        assertThat(result.getAttribute("jmespath_result")).isEqualTo("active");
    }
    
    @Test
    void testJmesPathNestedExpression() {
        Exchange<?, ?> result = processor.process(exchange, "${jmespath:user.id:userId}", Collections.emptyMap());
        assertThat(result.getAttribute("userId")).isEqualTo("123");
    }
    
    @Test
    void testJmesPathNonExistentPath() {
        Exchange<?, ?> result = processor.process(exchange, "${jmespath:user.nonexistent:result}", Collections.emptyMap());
        assertThat(result.getAttribute("result")).isEqualTo("null");
    }
    
    @Test
    void testJmesPathWithEmptyContent() {
        HttpExchange emptyExchange = new HttpExchange(
                new HttpRequest("http://example.com", HttpRequest.Method.GET),
                new HttpResponse(200, "OK"),
                100,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
        
        Exchange<?, ?> result = processor.process(emptyExchange, "${jmespath:user.name:result}", Collections.emptyMap());
        assertThat(result.getAttribute("result")).isEqualTo("");
    }
    
    @Test
    void testJmesPathWithInvalidJson() {
        HttpExchange invalidExchange = new HttpExchange(
                new HttpRequest("http://example.com", HttpRequest.Method.GET),
                new HttpResponse(200, "OK"),
                100,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
        invalidExchange.getResponse().setBodyAsString("invalid json");
        
        Exchange<?, ?> result = processor.process(invalidExchange, "${jmespath:user.name:result}", Collections.emptyMap());
        // Should return original exchange when JSON parsing fails
        assertThat(result).isEqualTo(invalidExchange);
    }
}