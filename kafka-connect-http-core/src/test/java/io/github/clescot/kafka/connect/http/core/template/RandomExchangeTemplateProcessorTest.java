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
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class RandomExchangeTemplateProcessorTest {

    private RandomExchangeTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new RandomExchangeTemplateProcessor();
        
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
    void testRandomPatternIsCorrect() {
        Pattern pattern = Pattern.compile("\\$\\{random\\.(\\w+)(?::(\\d+))?(?::(\\d+))?\\}");
        
        java.util.regex.Matcher matcher1 = pattern.matcher("${random.int}");
        boolean findResult1 = matcher1.find();
        assertThat(findResult1).isTrue();
        assertThat(matcher1.group(1)).isEqualTo("int");
        assertThat(matcher1.group(2)).isNull();
        assertThat(matcher1.group(3)).isNull();
        
        java.util.regex.Matcher matcher2 = pattern.matcher("${random.int:0:100}");
        boolean findResult2 = matcher2.find();
        assertThat(findResult2).isTrue();
        assertThat(matcher2.group(1)).isEqualTo("int");
        assertThat(matcher2.group(2)).isEqualTo("0");
        assertThat(matcher2.group(3)).isEqualTo("100");
        
        java.util.regex.Matcher matcher3 = pattern.matcher("${random.long}");
        boolean findResult3 = matcher3.find();
        assertThat(findResult3).isTrue();
        assertThat(matcher3.group(1)).isEqualTo("long");
        assertThat(matcher3.group(2)).isNull();
        assertThat(matcher3.group(3)).isNull();
    }

    @Test
    void testProcessWithRandomInt() {
        String template = "${random.int}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        assertThat(attributes).isNotEmpty();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_int_")) {
                found = true;
                int value = Integer.parseInt(entry.getValue().toString());
                assertThat(value).isBetween(0, 100);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomIntWithRange() {
        String template = "${random.int:10:50}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_int_")) {
                found = true;
                int value = Integer.parseInt(entry.getValue().toString());
                assertThat(value).isBetween(10, 50);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomLong() {
        String template = "${random.long}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_long_")) {
                found = true;
                long value = Long.parseLong(entry.getValue().toString());
                assertThat(value).isBetween(0L, 1000L);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomLongWithRange() {
        String template = "${random.long:1000000:9999999}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_long_")) {
                found = true;
                long value = Long.parseLong(entry.getValue().toString());
                assertThat(value).isBetween(1000000L, 9999999L);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomDouble() {
        String template = "${random.double}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_double_")) {
                found = true;
                double value = Double.parseDouble(entry.getValue().toString());
                assertThat(value).isBetween(0.0, 1.0);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomDoubleWithRange() {
        String template = "${random.double:10:20}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_double_")) {
                found = true;
                double value = Double.parseDouble(entry.getValue().toString());
                assertThat(value).isBetween(10.0, 20.0);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomUUID() {
        String template = "${random.uuid}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_uuid_")) {
                found = true;
                String value = entry.getValue().toString();
                assertThat(value).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomString() {
        String template = "${random.string:15}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_string_")) {
                found = true;
                String value = entry.getValue().toString();
                assertThat(value).hasSize(15);
                assertThat(value).matches("[A-Za-z0-9]+");
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomStringDefaultLength() {
        String template = "${random.string}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_string_")) {
                found = true;
                String value = entry.getValue().toString();
                assertThat(value).hasSize(10);
                assertThat(value).matches("[A-Za-z0-9]+");
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithRandomBoolean() {
        String template = "${random.boolean}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_boolean_")) {
                found = true;
                String value = entry.getValue().toString();
                assertThat(value).isIn("true", "false");
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("existing", "value");
        
        String template = "${random.int}";
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
    void testProcessWithMultipleRandomExpressions() {
        String template = "${random.int:1:10}${random.uuid}${random.string:5}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        assertThat(attributes.size()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void testProcessWithIntegerAlias() {
        String template = "${random.integer:0:50}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_integer_")) {
                found = true;
                int value = Integer.parseInt(entry.getValue().toString());
                assertThat(value).isBetween(0, 50);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithBoolAlias() {
        String template = "${random.bool}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_bool_")) {
                found = true;
                assertThat(entry.getValue().toString()).isIn("true", "false");
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithFloatAlias() {
        String template = "${random.float:0:10}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        Map<String, Object> attributes = httpProcessedExchange.getAttributes();
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            if (entry.getKey().startsWith("random_float_")) {
                found = true;
                double value = Double.parseDouble(entry.getValue().toString());
                assertThat(value).isBetween(0.0, 10.0);
                break;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    void testProcessWithInvalidNumberFormat() {
        String template = "${random.int:abc:100}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        assertThat(processedExchange).isEqualTo(testExchange);
    }

    @Test
    void testProcessWithLongRangeOverflow() {
        String template = "${random.long:0:9223372036854775807}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        boolean found = false;
        for (Map.Entry<String, Object> entry : httpProcessedExchange.getAttributes().entrySet()) {
            if (entry.getKey().startsWith("random_long_")) {
                found = true;
                long value = Long.parseLong(entry.getValue().toString());
                assertThat(value).isGreaterThanOrEqualTo(0L);
                break;
            }
        }
    }
}
