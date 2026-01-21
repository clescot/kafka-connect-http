package io.github.clescot.core.template;

import io.github.clescot.core.http.Exchange;
import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
import io.github.clescot.core.http.template.ConditionalTemplateProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionalTemplateProcessorTest {

    private ConditionalTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new ConditionalTemplateProcessor();

        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        request.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        request.setBodyAsString("{\"input\": \"test data\"}");

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
        assertThat(processor.getName()).isEqualTo("if");
    }

    @Test
    void testSupportsValidTemplates() {
        assertThat(processor.supports("${if:true:yes:no}")).isTrue();
        assertThat(processor.supports("${if:has:attr:true:false}")).isTrue();
        assertThat(processor.supports("${if:status:200:success:error}")).isTrue();
        assertThat(processor.supports("${if:value>5:high:low}")).isTrue();
    }

    @Test
    void testSupportsInvalidTemplates() {
        assertThat(processor.supports("${jsonpath:$.test}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports("${if}")).isFalse();
        assertThat(processor.supports("if:true:yes:no}")).isFalse();
    }

    @Test
    void testProcessWithTrueCondition() {
        String template = "${if:true:yes_value:no_value}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("yes_value");
    }

    @Test
    void testProcessWithFalseCondition() {
        String template = "${if:false:yes_value:no_value}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("no_value");
    }

    @Test
    void testProcessWithYesCondition() {
        String template = "${if:yes:yes_value:no_value}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("yes_value");
    }

    @Test
    void testProcessWithNoCondition() {
        String template = "${if:no:yes_value:no_value}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("no_value");
    }

    @Test
    void testProcessWithHasAttributeExisting() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("my_attribute", "some_value");
        String template = "${if:has:my_attribute:exists:missing}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("exists");
    }

    @Test
    void testProcessWithHasAttributeMissing() {
        String template = "${if:has:missing_attr:exists:missing}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("missing");
    }

    @Test
    void testProcessWithStatusEquals() {
        String template = "${if:status:==200:success:error}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("success");
    }

    @Test
    void testProcessWithStatusNotEqual() {
        String template = "${if:status:!=404:found:not_found}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("found");
    }

    @Test
    void testProcessWithStatusGreaterThan() {
        String template = "${if:status:>100:high:low}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("high");
    }

    @Test
    void testProcessWithStatusLessThan() {
        String template = "${if:status:<300:success:failure}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("success");
    }

    @Test
    void testProcessWithStatusGreaterThanOrEqual() {
        String template = "${if:status:>=200:ok:not_ok}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("ok");
    }

    @Test
    void testProcessWithStatusLessThanOrEqual() {
        String template = "${if:status:<=299:success:redirect}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("success");
    }

    @Test
    void testProcessWithStatusRange() {
        String template = "${if:status:200-299:success:error}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("success");
    }

    @Test
    void testProcessWithStatusRangeNotMatching() {
        String template = "${if:status:300-399:redirect:error}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("error");
    }

    @Test
    void testProcessWithNumericGreaterThan() {
        HttpExchange exchangeWithCount = (HttpExchange) testExchange.withAttribute("count", 10);
        String template = "${if:count>5:high:low}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithCount, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("high");
    }

    @Test
    void testProcessWithNumericLessThan() {
        HttpExchange exchangeWithCount = (HttpExchange) testExchange.withAttribute("count", 3);
        String template = "${if:count<5:low:high}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithCount, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("low");
    }

    @Test
    void testProcessWithNumericEquals() {
        HttpExchange exchangeWithValue = (HttpExchange) testExchange.withAttribute("value", 100);
        String template = "${if:value==100:match:no_match}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithValue, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("match");
    }

    @Test
    void testProcessWithNumericNotEquals() {
        HttpExchange exchangeWithValue = (HttpExchange) testExchange.withAttribute("value", 50);
        String template = "${if:value!=0:non_zero:zero}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithValue, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("non_zero");
    }

    @Test
    void testProcessWithNumericGreaterThanOrEqual() {
        HttpExchange exchangeWithScore = (HttpExchange) testExchange.withAttribute("score", 60);
        String template = "${if:score>=60:pass:fail}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithScore, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("pass");
    }

    @Test
    void testProcessWithNumericLessThanOrEqual() {
        HttpExchange exchangeWithAttempts = (HttpExchange) testExchange.withAttribute("attempts", 3);
        String template = "${if:attempts<=3:continue:stop}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttempts, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("continue");
    }

    @Test
    void testProcessWithExistingAttribute() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("my_attr", "some_value");
        String template = "${if:my_attr:found:not_found}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("found");
    }

    @Test
    void testProcessWithNonExistingAttribute() {
        String template = "${if:missing_attr:found:not_found}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("not_found");
    }

    @Test
    void testProcessWithEmptyStringAttribute() {
        HttpExchange exchangeWithEmptyAttr = (HttpExchange) testExchange.withAttribute("my_attr", "");
        String template = "${if:my_attr:has_value:no_value}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithEmptyAttr, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("no_value");
    }

    @Test
    void testProcessWithEmptyTrueValue() {
        String template = "${if:true::no_value}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessWithEmptyFalseValue() {
        String template = "${if:false:yes_value:}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessWithNumericAttributeConversion() {
        HttpExchange exchangeWithNum = (HttpExchange) testExchange.withAttribute("price", "19.99");
        String template = "${if:price>10:expensive:cheap}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithNum, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("expensive");
    }

    @Test
    void testProcessWithNonNumericAttributeInComparison() {
        HttpExchange exchangeWithText = (HttpExchange) testExchange.withAttribute("value", "not_a_number");
        String template = "${if:value>5:greater:less}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithText, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("less");
    }

    @Test
    void testProcessWithNumericComparisonAgainstLiteral() {
        String template = "${if:10>5:true:false}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("true");
    }

    @Test
    void testProcessWithDoubleNumericComparison() {
        HttpExchange exchangeWithDouble = (HttpExchange) testExchange.withAttribute("pi", 3.14159);
        String template = "${if:pi>3.0:above:below}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithDouble, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("above");
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttrs = testExchange
                .withAttribute("existing1", "value1")
                .withAttribute("existing2", "value2");

        String template = "${if:true:yes:no}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttrs, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getAttributes()).containsKey("existing1");
        assertThat(httpExchange.getAttributes()).containsKey("existing2");
        assertThat(httpExchange.getContent()).isEqualTo("yes");
    }

    @Test
    void testProcessWithSpacesInCondition() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("count", 10);
        String template = "${if:count > 5:high:low}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("high");
    }

    @Test
    void testProcessCaseInsensitiveYesNo() {
        String template = "${if:YES:yes_value:no_value}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("yes_value");
    }

    @Test
    void testProcessWithZeroValue() {
        HttpExchange exchangeWithZero = (HttpExchange) testExchange.withAttribute("count", 0);
        String template = "${if:count==0:zero:non_zero}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithZero, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("zero");
    }

    @Test
    void testProcessWithNegativeValue() {
        HttpExchange exchangeWithNegative = (HttpExchange) testExchange.withAttribute("temp", -10);
        String template = "${if:temp<0:freezing:warm}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithNegative, template, new HashMap<>());

        HttpExchange httpExchange = (HttpExchange) processedExchange;
        assertThat(httpExchange.getContent()).isEqualTo("freezing");
    }

    @Test
    void testProcessWithInvalidTemplateFormat() {
        String template = "${if:incomplete}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testProcessWithOnlyTwoParts() {
        String template = "${if:only_two}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testProcessWithMalformedComparison() {
        HttpExchange exchangeWithValue = (HttpExchange) testExchange.withAttribute("value", 10);
        String template = "${if:value>>5:high:low}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithValue, template, new HashMap<>());

        assertThat(processedExchange).isNotNull();
    }

    @Test
    void testProcessWithInvalidStatusCondition() {
        String template = "${if:status:>abc:success:error}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isNotNull();
    }
}
