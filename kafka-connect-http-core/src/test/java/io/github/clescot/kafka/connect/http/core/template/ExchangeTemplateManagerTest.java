package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExchangeTemplateManagerTest {

    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        request.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        request.setBodyAsString("{\"input\": \"test data\"}");

        HttpResponse response = new HttpResponse(200, "OK");
        response.getHeaders().put("Content-Type", Collections.singletonList("application/json"));
        response.setBodyAsString("{\"result\": \"success\", \"data\": \"processed data\"}");

        testExchange = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );
    }

    @Nested
    class ConstructorTests {

        @Test
        void testDefaultConstructorRegistersDefaultProcessors() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            assertThat(manager.getProcessors()).isNotEmpty();
            assertThat(manager.getProcessor("jsonpath")).isNotNull();
            assertThat(manager.getProcessor("xpath")).isNotNull();
            assertThat(manager.getProcessor("random")).isNotNull();
            assertThat(manager.getProcessor("jmespath")).isNotNull();
            assertThat(manager.getProcessor("regex")).isNotNull();
            assertThat(manager.getProcessor("headerparam")).isNotNull();
            assertThat(manager.getProcessor("datetime")).isNotNull();
            assertThat(manager.getProcessor("conditional")).isNotNull();
            assertThat(manager.getProcessor("hash")).isNotNull();
            assertThat(manager.getProcessor("math")).isNotNull();
        }

        @Test
        void testConstructorWithFalseDoesNotRegisterDefaults() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);

            assertThat(manager.getProcessors()).isEmpty();
        }

        @Test
        void testConstructorWithTrueRegistersDefaults() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(true);

            assertThat(manager.getProcessors()).isNotEmpty();
            assertThat(manager.getProcessor("jsonpath")).isNotNull();
        }
    }

    @Nested
    class ProcessorRegistrationTests {

        @Test
        void testRegisterProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
            JsonPathExchangeTemplateProcessor processor = new JsonPathExchangeTemplateProcessor();

            manager.registerProcessor(processor);

            assertThat(manager.getProcessors()).hasSize(1);
            assertThat(manager.getProcessor("jsonpath")).isEqualTo(processor);
        }

        @Test
        void testRegisterProcessorWithNullThrowsException() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);

            assertThatThrownBy(() -> manager.registerProcessor(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Processor cannot be null");
        }

        @Test
        void testRegisterDuplicateProcessorLogsWarning() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
            JsonPathExchangeTemplateProcessor processor1 = new JsonPathExchangeTemplateProcessor();
            JsonPathExchangeTemplateProcessor processor2 = new JsonPathExchangeTemplateProcessor();

            manager.registerProcessor(processor1);
            manager.registerProcessor(processor2);

            assertThat(manager.getProcessors()).hasSize(1);
        }

        @Test
        void testUnregisterProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();
            ExchangeTemplateProcessor processor = manager.getProcessor("jsonpath");

            manager.unregisterProcessor("jsonpath");

            assertThat(manager.getProcessor("jsonpath")).isNull();
        }

        @Test
        void testUnregisterNonexistentProcessorDoesNotThrow() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            manager.unregisterProcessor("nonexistent");

            assertThat(manager.getProcessors()).isNotEmpty();
        }

        @Test
        void testClearProcessors() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            manager.clearProcessors();

            assertThat(manager.getProcessors()).isEmpty();
            assertThat(manager.canProcess("${jsonpath:$.test}")).isFalse();
        }
    }

    @Nested
    class GetProcessorTests {

        @Test
        void testGetProcessorReturnsCorrectProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            ExchangeTemplateProcessor processor = manager.getProcessor("jsonpath");

            assertThat(processor).isNotNull();
            assertThat(processor.getName()).isEqualTo("jsonpath");
        }

        @Test
        void testGetProcessorReturnsNullForUnknownProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            ExchangeTemplateProcessor processor = manager.getProcessor("unknown");

            assertThat(processor).isNull();
        }
    }

    @Nested
    class CanProcessTests {

        @Test
        void testCanProcessReturnsTrueForSupportedTemplate() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            boolean result = manager.canProcess("${jsonpath:$.response.statusCode}");

            assertThat(result).isTrue();
        }

        @Test
        void testCanProcessReturnsTrueForMultipleProcessors() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            assertThat(manager.canProcess("${jsonpath:$.test}")).isTrue();
            assertThat(manager.canProcess("${random.int:1:100}")).isTrue();
            assertThat(manager.canProcess("${datetime:now}")).isTrue();
        }

        @Test
        void testCanProcessReturnsFalseForUnknownTemplate() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            boolean result = manager.canProcess("plain text without templates");

            assertThat(result).isFalse();
        }

        @Test
        void testCanProcessReturnsFalseWhenNoProcessorsRegistered() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);

            boolean result = manager.canProcess("${jsonpath:$.test}");

            assertThat(result).isFalse();
        }
    }

    @Nested
    class ResolveTemplateTests {

        @Test
        void testResolveTemplateWithJsonPath() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${jsonpath:$.response.statusCode}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("200");
        }

        @Test
        void testResolveTemplateWithJsonPathFromRequest() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${jsonpath:$.request.url}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("http://example.com/api/test");
        }

        @Test
        void testResolveTemplateWithRandom() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${random.int:1:100}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isNotEmpty();
            int value = Integer.parseInt(result);
            assertThat(value).isGreaterThanOrEqualTo(1);
            assertThat(value).isLessThanOrEqualTo(100);
        }

        @Test
        void testResolveTemplateWithMultipleExpressions() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Status: ${jsonpath:$.response.statusCode}, URL: ${jsonpath:$.request.url}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).contains("Status: 200");
            assertThat(result).contains("URL: http://example.com/api/test");
        }

        @Test
        void testResolveTemplatePreservesNonTemplateText() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Before ${jsonpath:$.response.statusCode} after";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Before 200 after");
        }

        @Test
        void testResolveTemplateWithNoProcessorsReturnsOriginal() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);

            String template = "${jsonpath:$.test}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo(template);
        }

        @Test
        void testResolveTemplateWithEmptyTemplateReturnsEmpty() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String result = manager.resolveTemplate(testExchange, "", new HashMap<>());

            assertThat(result).isEmpty();
        }

        @Test
        void testResolveTemplateWithNullTemplateReturnsNull() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String result = manager.resolveTemplate(testExchange, null, new HashMap<>());

            assertThat(result).isNull();
        }

        @Test
        void testResolveTemplateWithPlainTextReturnsUnchanged() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "plain text without templates";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo(template);
        }

        @Test
        void testResolveTemplateWithMultipleProcessors() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Status: ${jsonpath:$.response.statusCode}, URL: ${jsonpath:$.request.url}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).contains("Status: 200");
            assertThat(result).contains("URL: http://example.com/api/test");
        }

        @Test
        void testResolveTemplateWithUnresolvedExpressionKeepsOriginal() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
            manager.registerProcessor(new JsonPathExchangeTemplateProcessor());

            String template = "${unknownprocessor:something}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo(template);
        }

        @Test
        void testResolveTemplateWithNestedBraces() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Text with ${jsonpath:$.response.statusCode} and more text";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Text with 200 and more text");
        }

        @Test
        void testResolveTemplateWithDateTimeProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Current date: ${datetime:now:yyyy-MM-dd}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).contains("Current date:");
            assertThat(result).matches("Current date: \\d{4}-\\d{2}-\\d{2}");
        }
    }

    @Nested
    class GetProcessorsTests {

        @Test
        void testGetProcessorsReturnsUnmodifiableCollection() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            assertThat(manager.getProcessors()).isNotNull();
            assertThat(manager.getProcessors()).isUnmodifiable();
        }

        @Test
        void testGetProcessorsReturnsCorrectCount() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            assertThat(manager.getProcessors()).hasSizeGreaterThanOrEqualTo(10);
        }
    }

    @Nested
    class RegisterDefaultProcessorsTests {

        @Test
        void testRegisterDefaultProcessorsOnEmptyManager() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
            assertThat(manager.getProcessors()).isEmpty();

            manager.registerDefaultProcessors();

            assertThat(manager.getProcessors()).hasSizeGreaterThanOrEqualTo(10);
            assertThat(manager.getProcessor("jsonpath")).isNotNull();
            assertThat(manager.canProcess("${jsonpath:$.test}")).isTrue();
        }

        @Test
        void testRegisterDefaultProcessorsOnManagerWithProcessors() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
            manager.registerProcessor(new JsonPathExchangeTemplateProcessor());
            assertThat(manager.getProcessors()).hasSize(1);

            manager.registerDefaultProcessors();

            assertThat(manager.getProcessors()).hasSizeGreaterThanOrEqualTo(10);
        }
    }
}
