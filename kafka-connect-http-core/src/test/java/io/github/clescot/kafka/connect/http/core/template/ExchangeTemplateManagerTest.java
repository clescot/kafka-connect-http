package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
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

            assertThat(manager.getProcessors())
                    .isNotEmpty()
                    .hasSizeGreaterThanOrEqualTo(10);

            assertThat(manager.getProcessor("jsonpath")).isNotNull();
            assertThat(manager.getProcessor("xpath")).isNotNull();
            assertThat(manager.getProcessor("random")).isNotNull();
            assertThat(manager.getProcessor("jmespath")).isNotNull();
            assertThat(manager.getProcessor("regex")).isNotNull();
            assertThat(manager.getProcessor("header")).isNotNull();
            assertThat(manager.getProcessor("datetime")).isNotNull();
            assertThat(manager.getProcessor("if")).isNotNull();
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

            assertThat(manager.getProcessors())
                    .isNotEmpty()
                    .hasSizeGreaterThanOrEqualTo(10);

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

            assertThat(manager.getProcessors())
                    .hasSize(1)
                    .contains(processor);

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
            assertThat(manager.getProcessors())
                    .doesNotContain(processor)
                    .hasSize(manager.getProcessors().size());
        }

        @Test
        void testUnregisterNonexistentProcessorDoesNotThrow() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            assertThatNoException().isThrownBy(() -> manager.unregisterProcessor("nonexistent"));
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

            assertThat(processor)
                    .isNotNull()
                    .extracting(ExchangeTemplateProcessor::getName)
                    .isEqualTo("jsonpath");
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

            assertThat(manager.canProcess("${jsonpath:$.response.statusCode}")).isTrue();
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
            assertThat(value).isBetween(1, 100);
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
            Assertions.assertThrows(IllegalArgumentException.class,()->manager.resolveTemplate(testExchange, null, new HashMap<>()));
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

            assertThat(result)
                    .contains("Status: 200")
                    .contains("URL: http://example.com/api/test");
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

            assertThat(result)
                    .contains("Current date:")
                    .matches("Current date: \\d{4}-\\d{2}-\\d{2}");
        }

        @Test
        void testResolveTemplateWithNestedTextBlocks() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Start [Status: ${jsonpath:$.response.statusCode}] Middle [URL: ${jsonpath:$.request.url}] End";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Start [Status: 200] Middle [URL: http://example.com/api/test] End");
        }

        @Test
        void testResolveTemplateWithMultipleDifferentProcessors() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "ID: ${random.int:1000:9999} | Date: ${datetime:now:yyyy-MM-dd} | Status: ${jsonpath:$.response.statusCode}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("ID: ")
                    .contains(" | Date: ")
                    .contains(" | Status: 200")
                    .matches("ID: \\d+ \\| Date: \\d{4}-\\d{2}-\\d{2} \\| Status: 200");
        }

        @Test
        void testResolveTemplateWithConsecutiveTemplateExpressions() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${jsonpath:$.response.statusCode}${jsonpath:$.request.url}${random.int:1:10}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .startsWith("200")
                    .contains("http://example.com/api/test");

            String lastPart = result.substring(result.length() - 1);
            assertThat(Integer.parseInt(lastPart)).isBetween(1, 10);
        }

        @Test
        void testResolveTemplateWithSpecialCharactersAroundTemplates() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "### ${jsonpath:$.response.statusCode} ###";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("### 200 ###");
        }

        @Test
        void testResolveTemplateWithMultipleJsonPathExpressions() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${jsonpath:$.response.statusCode}|${jsonpath:$.request.url}|${jsonpath:$.response.body}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .startsWith("200|http://example.com/api/test|")
                    .contains("\"result\"")
                    .contains("\"success\"")
                    .contains("\"data\"")
                    .contains("\"processed data\"");
        }

        @Test
        void testResolveTemplateWithEmptyTextBetweenTemplates() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${jsonpath:$.response.statusCode}  ${jsonpath:$.request.url}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("200  http://example.com/api/test");
        }

        @Test
        void testResolveTemplateWithNewlinesAndTemplates() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Line1: ${jsonpath:$.response.statusCode}\nLine2: ${jsonpath:$.request.url}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Line1: 200\nLine2: http://example.com/api/test");
        }

        @Test
        void testResolveTemplateWithMixedTemplatesAndText() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "GET ${jsonpath:$.request.url} returned status ${jsonpath:$.response.statusCode} at ${datetime:now}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("GET http://example.com/api/test returned status 200 at ")
                    .hasSizeGreaterThan("GET http://example.com/api/test returned status 200 at ".length());
        }

        @Test
        void testResolveTemplateWithHashProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Hash: ${hash:MD5:${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .startsWith("Hash: ")
                    .hasSizeGreaterThan("Hash: ".length());
        }

        @Test
        void testResolveTemplateWithMathProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Calculation: ${math:10+5}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).startsWith("Calculation: 15");
        }

        @Test
        void testResolveTemplateWithConditionalProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Result: ${if:status:==200:success:failure}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Result: success");
        }

        @Test
        void testResolveTemplateWithHeaderProcessor() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Content-Type: ${header:Content-Type}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Content-Type: application/json");
        }

        @Test
        void testResolveTemplateWithMultipleProcessorsInOneLine() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${header:Content-Type}|${jsonpath:$.response.statusCode}|${random.int:100:200}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).startsWith("application/json|200|");

            String randomPart = result.substring(result.lastIndexOf('|') + 1);
            int randomValue = Integer.parseInt(randomPart);
            assertThat(randomValue).isBetween(100, 200);
        }

        @Test
        void testResolveTemplateWithNestedConditionalShowsLimitation() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:status:==200:${jsonpath:$.response.statusCode}:${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Nested conditional should now resolve inner templates (limitation fixed)")
                    .isEqualTo("200");
        }


        @Test
        void testResolveTemplateWithConditionalFalseBranchNotRecursive() {
            HttpRequest failRequest = new HttpRequest("http://example.com/api/fail", HttpRequest.Method.GET);
            failRequest.setBodyAsString("{}");
            HttpResponse failResponse = new HttpResponse(404, "Not Found");
            HttpExchange failExchange = new HttpExchange(
                    failRequest,
                    failResponse,
                    50L,
                    OffsetDateTime.now(),
                    new AtomicInteger(1),
                    true
            );

            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Result: ${if:status:==200:success:error}";
            String result = manager.resolveTemplate(failExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Result: error");
        }

        @Test
        void testResolveTemplateWithConditionalAndJsonPathSequential() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:status:==200:Success:Failed} - Status: ${jsonpath:$.response.statusCode}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Success - Status: 200");
        }

        @Test
        void testResolveTemplateWithConditionalInsideTextBlock() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "[IF_SUCCESS: ${if:status:==200:✓:✗}] [Status: ${jsonpath:$.response.statusCode}]";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("[IF_SUCCESS: ✓] [Status: 200]");
        }

        @Test
        void testResolveTemplateWithMathAndConditional() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Math: ${math:10+5} | Check: ${if:true:pass:fail}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).startsWith("Math: 15.0 | Check: pass");
        }

        @Test
        void testResolveTemplateWithHashAndConditional() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Hash: ${hash:MD5:${jsonpath:$.request.url}} | Valid: ${if:true:yes:no}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("Hash: ")
                    .contains(" | Valid: yes");
        }

        @Test
        void testResolveTemplateWithDateTimeAndConditional() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Date: ${datetime:now:yyyy-MM-dd} | Status: ${if:status:==200:active:inactive}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("Date: ")
                    .contains(" | Status: active");
        }

        @Test
        void testResolveTemplateWithRandomAndConditional() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Random: ${random.int:1:100} | Decision: ${if:true:proceed:abort}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("Random: ")
                    .contains(" | Decision: proceed");
        }

        @Test
        void testResolveTemplateWithFourProcessorsMixed() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${header:Content-Type}|${jsonpath:$.response.statusCode}|${datetime:now:HH:mm:ss}|${if:true:A:B}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .startsWith("application/json|200|")
                    .contains("|A");
        }

        @Test
        void testResolveTemplateWithConditionalAndJsonPathBody() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Result: ${if:status:>=200:${jsonpath:$.response.body}:invalid}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("Result: ")
                    .contains("\"result\"");
        }

        @Test
        void testResolveTemplateWithMultipleConditionals() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "[C1: ${if:true:A:B}] [C2: ${if:status:==200:X:Y}] [C3: ${if:false:P:Q}]";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("[C1: A] [C2: X] [C3: Q]");
        }

        @Test
        void testResolveTemplateWithConditionalAndHeader() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Header: ${header:Content-Type} | Match: ${if:header:Content-Type:present:missing}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).isEqualTo("Header: application/json | Match: present");
        }

        @Test
        void testResolveTemplateWithConditionalRangeAndMultipleProcessors() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Status: ${jsonpath:$.response.statusCode} | Range: ${if:status:200-299:in_range:out_of_range} | Date: ${datetime:now}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("Status: 200 | Range: in_range | Date: ")
                    .hasSizeGreaterThan("Status: 200 | Range: in_range | Date: ".length());
        }

        @Test
        void testResolveTemplateWithHashProcessorNotRecursive() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Hash: ${hash:MD5:${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("Hash: ")
                    .doesNotContain("${jsonpath:$.request.url}");
        }

        @Test
        void testResolveTemplateWithMathProcessorNotRecursive() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Math: ${math:${jsonpath:$.response.statusCode}+1}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .contains("Math: ")
                    .doesNotContain("${jsonpath:");
        }

        @Test
        void testResolveTemplateWithMultipleProcessorsSequential() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${header:Content-Type}|${jsonpath:$.response.statusCode}|${if:true:A:B}|${random.int:1:10}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result).startsWith("application/json|200|A|");

            String randomPart = result.substring(result.lastIndexOf('|') + 1);
            int randomValue = Integer.parseInt(randomPart);
            assertThat(randomValue).isBetween(1, 10);
        }

        @Test
        void testResolveTemplateLimitationNestedJsonPath() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Value: ${jsonpath:$.${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .isNotEqualTo("Value: ${jsonpath:$.http://example.com/api/test}")
                    .contains("Value: ");
        }
    }

    @Nested
    class GetProcessorsTests {

        @Test
        void testGetProcessorsReturnsUnmodifiableCollection() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            assertThat(manager.getProcessors())
                    .isNotNull()
                    .isUnmodifiable();
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

            assertThat(manager.getProcessors())
                    .hasSizeGreaterThanOrEqualTo(10);

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

    @Nested
    class ResolveTemplateLimitationTests {

        @Test
        void testResolveTemplateNestedConditionalShouldRecursivelyProcess() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:status:==200:${jsonpath:$.response.statusCode}:${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Nested conditional should recursively resolve inner templates")
                    .isEqualTo("200");
        }

        @Test
        void testResolveTemplateConditionalTrueBranchShouldResolveInnerTemplate() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Result: ${if:status:==200:status_is_${jsonpath:$.response.statusCode}:error}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Conditional true branch should resolve inner jsonpath template")
                    .isEqualTo("Result: status_is_200");
        }

        @Test
        void testResolveTemplateConditionalFalseBranchShouldResolveInnerTemplate() {
            HttpRequest failRequest = new HttpRequest("http://example.com/api/fail", HttpRequest.Method.GET);
            failRequest.setBodyAsString("{}");
            HttpResponse failResponse = new HttpResponse(404, "Not Found");
            HttpExchange failExchange = new HttpExchange(
                    failRequest,
                    failResponse,
                    50L,
                    OffsetDateTime.now(),
                    new AtomicInteger(1),
                    true
            );

            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Result: ${if:status:==200:success:error_code_${jsonpath:$.response.statusCode}}";
            String result = manager.resolveTemplate(failExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Conditional false branch should resolve inner jsonpath template")
                    .isEqualTo("Result: error_code_404");
        }

        @Test
        void testResolveTemplateHashProcessorShouldRecursivelyResolveInnerTemplate() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Hash: ${hash:MD5:${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Hash processor should recursively resolve inner jsonpath template before hashing")
                    .isEqualTo("Hash: c044f31eceb5f72813b7afe72b81c2ae");
        }

        @Test
        void testResolveTemplateMathProcessorShouldRecursivelyResolveInnerTemplate() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Math: ${math:${jsonpath:$.response.statusCode}+1}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Math processor should recursively resolve inner jsonpath template before calculating")
                    .isEqualTo("Math: 201.0");
        }

        @Test
        void testResolveTemplateNestedJsonPathShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "Value: ${jsonpath:$.${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Nested jsonpath should be resolved recursively: outer extracts key from inner result")
                    .isNotNull()
                    .isNotEmpty();
        }

        @Test
        void testResolveTemplateDeeplyNestedConditionalsShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:status:==200:${if:true:${jsonpath:$.response.statusCode}:inner}:outer}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Deeply nested conditionals should resolve recursively")
                    .isEqualTo("200");
        }

        @Test
        void testResolveTemplateConditionalWithMathInnerShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:status:==200:${math:100+${jsonpath:$.response.statusCode}}:0}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Conditional true branch should resolve math with inner jsonpath")
                    .isEqualTo("300.0");
        }

        @Test
        void testResolveTemplateHeaderInsideConditionalShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:header:Content-Type:${header:Content-Type}:missing}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Header processor inside conditional should resolve recursively")
                    .isEqualTo("application/json");
        }

        @Test
        void testResolveTemplateRandomInsideConditionalShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:true:random_val_${random.int:1:100}:fixed}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Random processor inside conditional should resolve recursively")
                    .matches("random_val_\\d+");
        }

        @Test
        void testResolveTemplateDateTimeInsideConditionalShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:status:==200:timestamp_${datetime:now:yyyyMMdd}:invalid}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("DateTime processor inside conditional should resolve recursively")
                    .matches("timestamp_\\d{8}");
        }

        @Test
        void testResolveTemplateHashInsideMathShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${math:${hash:MD5:${jsonpath:$.request.url}}+1}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Hash inside math should resolve recursively")
                    .isNotNull()
                    .isNotEmpty();
        }

        @Test
        void testResolveTemplateMultipleNestedProcessorsShouldRecursivelyResolve() {
            ExchangeTemplateManager manager = new ExchangeTemplateManager();

            String template = "${if:status:==200:${hash:MD5:${jsonpath:$.request.url}}:${jsonpath:$.request.url}}";
            String result = manager.resolveTemplate(testExchange, template, new HashMap<>());

            assertThat(result)
                    .as("Hash inside conditional inside jsonpath should resolve recursively")
                    .isNotNull()
                    .isNotEmpty()
                    .doesNotContain("${");
        }
    }
}
