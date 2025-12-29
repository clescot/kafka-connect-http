package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Regex template processor for extracting data from exchange content using regular expressions.
 * Useful for parsing unstructured or semi-structured data.
 */
public class RegexExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(RegexExchangeTemplateProcessor.class);
    
    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            // Extract regex pattern and attribute name from template
            // Template format: ${regex:pattern:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 2) {
                log.warn("Invalid regex template format: {}", template);
                return   exchange;
            }
            
            String regexPattern = parts[0];
            String attributeName = parts.length > 1 ? parts[1] : "regex_result";
            
            // Get content to search
            String content = exchange.getContentAsString();
            if (content == null || content.trim().isEmpty()) {
                log.debug("No content available for regex processing");
                return   exchange.withAttribute(attributeName, "");
            }
            
            // Compile and apply regex pattern
            Pattern pattern = Pattern.compile(regexPattern);
            Matcher matcher = pattern.matcher(content);
            
            String result;
            if (matcher.find()) {
                // Use group 1 if available, otherwise use entire match
                result = matcher.groupCount() > 0 ? matcher.group(1) : matcher.group(0);
                log.debug("Regex pattern '{}' matched: {}", regexPattern, result);
            } else {
                result = "";
                log.debug("Regex pattern '{}' did not match any content", regexPattern);
            }
            
            return   exchange.withAttribute(attributeName, result);
            
        } catch (PatternSyntaxException e) {
            log.warn("Invalid regex pattern in template '{}': {}", template, e.getMessage());
            return   exchange;
        } catch (Exception e) {
            log.warn("Failed to process regex template '{}': {}", template, e.getMessage());
            return   exchange;
        }
    }
    
    @Override
    public boolean supports(@NotNull String template) {
        return template.startsWith("${regex:") && template.contains(":");
    }
    
    @Override
    public String getName() {
        return "regex";
    }
    
    /**
     * Extract parts from template: ${regex:pattern:attributeName}
     * Returns array where [0] = pattern, [1] = attributeName (if present)
     */
    private String[] extractTemplateParts(String template) {
        // Remove ${regex: and }
        String innerContent = template.substring("${regex:".length(), template.length() - 1);
        
        // Split by colon, but handle escaped colons in regex patterns
        int lastColonIndex = findLastUnescapedColon(innerContent);
        
        if (lastColonIndex > 0) {
            String pattern = innerContent.substring(0, lastColonIndex);
            String attributeName = innerContent.substring(lastColonIndex + 1);
            return new String[]{pattern, attributeName};
        } else {
            return new String[]{innerContent};
        }
    }
    
    /**
     * Find the last unescaped colon in a string (colons in regex patterns can be escaped with backslash)
     */
    private int findLastUnescapedColon(String str) {
        for (int i = str.length() - 1; i >= 0; i--) {
            if (str.charAt(i) == ':' && (i == 0 || str.charAt(i - 1) != '\\')) {
                return i;
            }
        }
        return -1;
    }
}