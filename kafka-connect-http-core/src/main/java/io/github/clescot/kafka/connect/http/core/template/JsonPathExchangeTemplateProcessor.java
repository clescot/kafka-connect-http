package io.github.clescot.kafka.connect.http.core.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JSONPath template processor for Exchange.
 * Allows extracting and transforming data from any Exchange implementation using JSONPath expressions.
 */
public class JsonPathExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathExchangeTemplateProcessor.class);
    
    public static final String NAME = "jsonpath";
    private static final Pattern JSONPATH_PATTERN = Pattern.compile("\\$\\{jsonpath:(.*?)\\}");
    
    // Configure JSONPath with Jackson
    private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL)
            .build();

    @Override
    public Exchange<?, ?> process(@NotNull Exchange<?, ?> exchange, @NotNull String template, Map<String, Object> context) {
        LOGGER.debug("Processing template with JSONPath: {}", template);
        
        // Start with the original exchange
        Exchange<?, ?> modifiedExchange = exchange;
        
        // Process the template to extract JSONPath expressions
        Matcher matcher = JSONPATH_PATTERN.matcher(template);
        
        LOGGER.debug("Looking for JSONPath patterns in template: {}", template);
        boolean foundAny = false;
        
        while (matcher.find()) {
            foundAny = true;
            String jsonPathExpression = matcher.group(1);
            LOGGER.debug("Found JSONPath expression: {}", jsonPathExpression);
            
            try {
                // Try to evaluate the JSONPath expression against the exchange
                Object result = evaluateJsonPath(exchange, jsonPathExpression);
                
                if (result != null) {
                    LOGGER.debug("JSONPath result for '{}': {}", jsonPathExpression, result);
                    
                    // Extract the raw value from JSONPath result
                    String resultValue;
                    if (result instanceof com.fasterxml.jackson.databind.JsonNode) {
                        // If it's a Jackson JsonNode, get the raw value
                        com.fasterxml.jackson.databind.JsonNode jsonNode = (com.fasterxml.jackson.databind.JsonNode) result;
                        if (jsonNode.isTextual()) {
                            resultValue = jsonNode.asText();
                        } else if (jsonNode.isNumber()) {
                            resultValue = jsonNode.asText();
                        } else if (jsonNode.isBoolean()) {
                            resultValue = jsonNode.asText();
                        } else {
                            resultValue = jsonNode.toString();
                        }
                    } else {
                        resultValue = result.toString();
                    }
                    
                    // Add the result to attributes using the exchange's withAttribute method
                    String attributeName = "jsonpath_" + jsonPathExpression.replaceAll("[^a-zA-Z0-9_]", "_");
                    modifiedExchange = modifiedExchange.withAttribute(attributeName, resultValue);
                    LOGGER.debug("Added attribute: {} = {}", attributeName, resultValue);
                } else {
                    LOGGER.debug("JSONPath expression '{}' returned null", jsonPathExpression);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to evaluate JSONPath expression '{}': {}", jsonPathExpression, e.getMessage());
                LOGGER.debug("Exception details:", e);
            }
        }
        
        if (!foundAny) {
            LOGGER.debug("No JSONPath expressions found in template");
        }
        
        return modifiedExchange;
    }

    /**
     * Evaluate a JSONPath expression against the Exchange.
     * 
     * @param exchange the exchange to evaluate against
     * @param jsonPathExpression the JSONPath expression
     * @return the result of the evaluation, or null if failed
     */
    private Object evaluateJsonPath(Exchange<?, ?> exchange, String jsonPathExpression) {
        try {
            // Convert the Exchange to a JSON structure that JSONPath can navigate
            Object exchangeData = createExchangeMap(exchange);
            
            LOGGER.debug("Exchange data for JSONPath: {}", exchangeData);
            
            // Parse and evaluate the JSONPath expression
            JsonPath jsonPath = JsonPath.compile(jsonPathExpression);
            Object result = jsonPath.read(exchangeData, JSON_PATH_CONFIG);
            
            LOGGER.debug("JSONPath '{}' evaluated to: {}", jsonPathExpression, result);
            
            return result;
            
        } catch (Exception e) {
            LOGGER.debug("JSONPath evaluation failed for '{}': {}", jsonPathExpression, e.getMessage());
            LOGGER.debug("Exception details:", e);
            return null;
        }
    }

    /**
     * Create a JSON node representation of the Exchange for JSONPath navigation.
     * 
     * @param exchange the exchange to convert
     * @return JSON node representation of the exchange
     */
    private Object createExchangeMap(Exchange<?, ?> exchange) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode exchangeNode = objectMapper.createObjectNode();
            
            // Add basic exchange properties using the Exchange interface methods
            Map<String, Object> metadata = exchange.getMetadata();
            for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                exchangeNode.set(entry.getKey(), objectMapper.valueToTree(entry.getValue()));
            }
            
            // Add content
            exchangeNode.put("content", exchange.getContentAsString());
            
            // Add attributes
            exchangeNode.set("attributes", objectMapper.valueToTree(exchange.getAttributes()));
            
            return exchangeNode;
            
        } catch (Exception e) {
            LOGGER.warn("Failed to create JSON exchange map: {}", e.getMessage());
            // Fallback to simple map structure
            Map<String, Object> exchangeMap = new HashMap<>();
            
            // Add metadata
            exchangeMap.putAll(exchange.getMetadata());
            
            // Add content
            exchangeMap.put("content", exchange.getContentAsString());
            
            // Add attributes
            exchangeMap.put("attributes", exchange.getAttributes());
            
            return exchangeMap;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supports(String template) {
        return template != null && JSONPATH_PATTERN.matcher(template).find();
    }
}