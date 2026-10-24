package io.github.clescot.core.template;

import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for template pattern matching functionality
 */
class TemplatePatternTest {

    /**
     * Test JSONPath pattern matching
     */
    @Test
    void testJsonPathPatternMatching() {
        String template = "${jsonpath:$.response.statusCode} ${jsonpath:$.request.url}";
        Pattern pattern = Pattern.compile("\\$\\{jsonpath:(.*?)\\}");
        Matcher matcher = pattern.matcher(template);
        
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(0)).isEqualTo("${jsonpath:$.response.statusCode}");
        assertThat(matcher.group(1)).isEqualTo("$.response.statusCode");
        
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(0)).isEqualTo("${jsonpath:$.request.url}");
        assertThat(matcher.group(1)).isEqualTo("$.request.url");
        
        assertThat(matcher.find()).isFalse();
    }
    
    /**
     * Test Random pattern matching
     */
    @Test
    void testRandomPatternMatching() {
        String template = "${random.int:1:100} ${random.uuid}";
        Pattern pattern = Pattern.compile("\\$\\{random\\.(\\w+)(?::(\\d+))?(?::(\\d+))?\\}");
        Matcher matcher = pattern.matcher(template);
        
        // First match: ${random.int:1:100}
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(0)).isEqualTo("${random.int:1:100}");
        assertThat(matcher.group(1)).isEqualTo("int");
        assertThat(matcher.group(2)).isEqualTo("1");
        assertThat(matcher.group(3)).isEqualTo("100");
        
        // Second match: ${random.uuid}
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(0)).isEqualTo("${random.uuid}");
        assertThat(matcher.group(1)).isEqualTo("uuid");
        assertThat(matcher.group(2)).isNull();
        assertThat(matcher.group(3)).isNull();
        
        assertThat(matcher.find()).isFalse();
    }
    
    /**
     * Test XPath pattern matching
     */
    @Test
    void testXPathPatternMatching() {
        String template = "${xpath://response/statusCode}";
        Pattern pattern = Pattern.compile("\\$\\{xpath:(.*?)\\}");
        Matcher matcher = pattern.matcher(template);
        
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(0)).isEqualTo("${xpath://response/statusCode}");
        assertThat(matcher.group(1)).isEqualTo("//response/statusCode");
        
        assertThat(matcher.find()).isFalse();
    }
    
    /**
     * Test mixed template patterns
     */
    @Test
    void testMixedTemplatePatterns() {
        String template = "${jsonpath:$.response.statusCode} - ${random.uuid} - ${xpath://response/status}";
        
        // Test JSONPath pattern
        Pattern jsonPathPattern = Pattern.compile("\\$\\{jsonpath:(.*?)\\}");
        Matcher jsonPathMatcher = jsonPathPattern.matcher(template);
        assertThat(jsonPathMatcher.find()).isTrue();
        assertThat(jsonPathMatcher.group(1)).isEqualTo("$.response.statusCode");
        
        // Test Random pattern
        Pattern randomPattern = Pattern.compile("\\$\\{random\\.(\\w+)(?::(\\d+))?(?::(\\d+))?\\}");
        Matcher randomMatcher = randomPattern.matcher(template);
        assertThat(randomMatcher.find()).isTrue();
        assertThat(randomMatcher.group(1)).isEqualTo("uuid");
        
        // Test XPath pattern
        Pattern xpathPattern = Pattern.compile("\\$\\{xpath:(.*?)\\}");
        Matcher xpathMatcher = xpathPattern.matcher(template);
        assertThat(xpathMatcher.find()).isTrue();
        assertThat(xpathMatcher.group(1)).isEqualTo("//response/status");
    }
    
    /**
     * Test template without patterns
     */
    @Test
    void testPlainTextTemplate() {
        String template = "This is plain text without any patterns";
        Pattern pattern = Pattern.compile("\\$\\{jsonpath:(.*?)\\}");
        Matcher matcher = pattern.matcher(template);
        
        assertThat(matcher.find()).isFalse();
    }
}