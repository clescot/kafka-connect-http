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
        
        assertThat(httpProcessedExchange.getAttributes()).containsKey("math_result");
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("5.0");
    }

    @Test
    void testProcessWithSimpleSubtraction() {
        String template = "${math:10-4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("6.0");
    }

    @Test
    void testProcessWithMultiplication() {
        String template = "${math:6*7}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("42.0");
    }

    @Test
    void testProcessWithDivision() {
        String template = "${math:20/4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("5.0");
    }

    @Test
    void testProcessWithDecimalResult() {
        String template = "${math:10/3}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String result = httpProcessedExchange.getAttributes().get("math_result").toString();
        assertThat(Double.parseDouble(result)).isCloseTo(3.333, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    void testProcessWithParentheses() {
        String template = "${math:(2+3)*4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("20.0");
    }

    @Test
    void testProcessWithNestedParentheses() {
        String template = "${math:((1+2)*(3+4))}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("21.0");
    }

    @Test
    void testProcessWithMixedOperations() {
        String template = "${math:2+3*4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("14.0");
    }

    @Test
    void testProcessWithCustomAttributeName() {
        String template = "${math:100/10:calculated_result}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("calculated_result");
        assertThat(httpProcessedExchange.getAttributes().get("calculated_result").toString()).isEqualTo("10.0");
    }

    @Test
    void testProcessWithAttributeReference() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("multiplier", "5");
        
        String template = "${math:10*{multiplier}}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("50.0");
    }

    @Test
    void testProcessWithDollarAttributeReference() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("baseValue", "25");
        
        String template = "${math:${baseValue}*2}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("50.0");
    }

    @Test
    void testProcessWithMissingAttribute() {
        String template = "${math:10*{missing_attr}}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("0.0");
    }

    @Test
    void testProcessWithNegativeNumbers() {
        String template = "${math:10-5}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("5.0");
    }

    @Test
    void testProcessWithLeadingNegativeResult() {
        String template = "${math:5-10}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("-5.0");
    }

    @Test
    void testProcessWithDecimalNumbers() {
        String template = "${math:2.5*4}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("10.0");
    }

    @Test
    void testProcessWithInvalidExpression() {
        String template = "${math:not_a_number}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithComplexExpression() {
        String template = "${math:(100-50)/2+25}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("50.0");
    }

    @Test
    void testProcessWithMultipleOperations() {
        String template = "${math:1+2+3+4+5}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("15.0");
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("existing", "value");
        
        String template = "${math:5+5}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("existing");
        assertThat(httpProcessedExchange.getAttributes().get("existing").toString()).isEqualTo("value");
        assertThat(httpProcessedExchange.getAttributes()).containsKey("math_result");
    }

    @Test
    void testProcessWithEmptyTemplateContent() {
        String template = "${math::}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithDivisionByZero() {
        String template = "${math:10/0}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithWhitespaceInExpression() {
        String template = "${math: 5 + 3 }";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("8.0");
    }

    @Test
    void testProcessWithMultipleAttributeReferences() {
        HttpExchange exchangeWithAttrs = (HttpExchange) testExchange
                .withAttribute("a", "10")
                .withAttribute("b", "5");
        
        String template = "${math:${a}+${b}}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttrs, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("math_result").toString()).isEqualTo("15.0");
    }

    @Test
    void testProcessWithDecimalAttribute() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("price", "19.99");
        
        String template = "${math:{price}*2}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String result = httpProcessedExchange.getAttributes().get("math_result").toString();
        assertThat(Double.parseDouble(result)).isCloseTo(39.98, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    void testProcessWithComplexAttributeAndLiteral() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("responseTime", "150");
        
        String template = "${math:{responseTime}/1000:seconds}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("seconds");
        assertThat(httpProcessedExchange.getAttributes().get("seconds").toString()).isEqualTo("0.15");
    }
}
