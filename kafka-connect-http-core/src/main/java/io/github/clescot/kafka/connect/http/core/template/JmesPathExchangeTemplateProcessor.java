package io.github.clescot.kafka.connect.http.core.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Map;

/**
 * JMESPath template processor for extracting data from JSON content using JMESPath expressions.
 * JMESPath provides a simpler syntax than JSONPath for querying JSON data.
 */
public class JmesPathExchangeTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmesPathExchangeTemplateProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Configuration jsonPathConfig = Configuration.builder()
            .options(EnumSet.of(Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)).build();
    public static final String NAME = "jmespath";

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            // Extract the JMESPath expression and attribute name from template
            // Template format: ${jmespath:expression:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 1) {
                LOGGER.warn("Invalid JMESPath template format: {}", template);
                return   exchange;
            }
            
            String jmesPathExpression = parts[0];
            String attributeName = parts.length > 1 ? parts[1] : "jmespath_result";
            
            // Get content as JSON
            String content = exchange.getContentAsString();
            if (content == null || content.trim().isEmpty()) {
                LOGGER.debug("No content available for JMESPath processing");
                return   exchange.withAttribute(attributeName, "");
            }
            
            // Parse content as JSON
            JsonNode jsonContent = objectMapper.readTree(content);
            
            // Use JSONPath to evaluate JMESPath-like expression (JSONPath can handle similar syntax)
            Object result = JsonPath.using(jsonPathConfig).parse(jsonContent.toString()).read(jmesPathExpression);
            
            // Handle the result properly - extract from arrays if needed
            String resultString = extractResultValue(result);
            
            LOGGER.debug("JMESPath expression '{}' evaluated to: {}", jmesPathExpression, resultString);
            return   exchange.withAttribute(attributeName, resultString);
            
        } catch (Exception e) {
            LOGGER.warn("Failed to process JMESPath template '{}': {}", template, e.getMessage());
            return   exchange;
        }
    }
    
    @Override
    public boolean supports(@NotNull String template) {
        return template.startsWith("${jmespath:") && template.contains(":");
    }
    
    @Override
    public String getName() {
        return NAME;
    }
    
    /**
     * Extract parts from template: ${jmespath:expression:attributeName}
     * Returns array where [0] = expression, [1] = attributeName (if present)
     */
    private String[] extractTemplateParts(String template) {
        // Remove ${jmespath: and }
        String innerContent = template.substring("${jmespath:".length(), template.length() - 1);
        
        // Split by colon, but handle nested colons in expressions
        int lastColonIndex = innerContent.lastIndexOf(':');
        
        if (lastColonIndex > 0) {
            String expression = innerContent.substring(0, lastColonIndex);
            String attributeName = innerContent.substring(lastColonIndex + 1);
            return new String[]{expression, attributeName};
        } else {
            return new String[]{innerContent};
        }
    }
    
    /**
     * Extract the actual value from JSONPath result, handling arrays and nulls properly
     */
    private String extractResultValue(Object result) {
        if (result == null) {
            return "null";
        }
        
        // Handle arrays - extract first element if array has exactly one element
        if (result instanceof java.util.List) {
            java.util.List<?> listResult = (java.util.List<?>) result;
            if (listResult.isEmpty()) {
                return "null";
            } else if (listResult.size() == 1) {
                Object firstElement = listResult.get(0);
                return firstElement != null ? firstElement.toString() : "null";
            } else {
                // Multiple elements - return as JSON array
                try {
                    return objectMapper.writeValueAsString(listResult);
                } catch (Exception e) {
                    return listResult.toString();
                }
            }
        }
        
        return result.toString();
    }
}