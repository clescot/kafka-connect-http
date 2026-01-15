package io.github.clescot.kafka.connect.http.core.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.*;

/**
 * JSONPath template processor for Exchange.
 * Allows extracting and transforming data from any Exchange implementation using JSONPath expressions.
 */
public class JsonPathExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathExchangeTemplateProcessor.class);
    
    // Security constants
    private static final long JSONPATH_TIMEOUT_MS = 5000; // 5 second timeout
    private static final int MAX_TEMPLATE_LENGTH = 1000; // 1000 characters max
    private static final int MAX_RESULT_LENGTH = 10000; // 10000 characters max
    private static final int MAX_ATTRIBUTE_NAME_LENGTH = 100; // 100 characters max
    
    private static final ExecutorService jsonPathExecutor = Executors.newCachedThreadPool();
    
    public static final String NAME = "jsonpath";
    private static final Pattern JSONPATH_PATTERN = Pattern.compile("\\$\\{jsonpath:(.*?)\\}");
    
    // Configure JSONPath with Jackson
    private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL)
            .build();

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        LOGGER.debug("Processing template with JSONPath: {}", template);
        
        // Security validation
        if (template.length() > MAX_TEMPLATE_LENGTH) {
            LOGGER.warn("Template too long ({} characters), max allowed is {}: {}", 
                template.length(), MAX_TEMPLATE_LENGTH, template.substring(0, 100) + "...");
            return exchange;
        }

        // Process the template to extract JSONPath expressions
        Matcher matcher = JSONPATH_PATTERN.matcher(template);
        
        LOGGER.debug("Looking for JSONPath patterns in template: {}", template);
        boolean foundAny = false;
        Exchange<R, S> modifiedExchange = exchange;
        
        while (matcher.find()) {
            foundAny = true;
            String jsonPathExpression = matcher.group(1);
            LOGGER.debug("Found JSONPath expression: {}", jsonPathExpression);
            
            try {
                // Try to evaluate the JSONPath expression against the exchange
                Object result = evaluateJsonPath(modifiedExchange, jsonPathExpression);
                
                if (result != null) {
                    LOGGER.debug("JSONPath result for '{}': {}", jsonPathExpression, result);
                    
                    // Extract the raw value from JSONPath result
                    String resultValue;
                    // If it's a Jackson JsonNode, get the raw value
                    if (result instanceof com.fasterxml.jackson.databind.JsonNode jsonNode) {
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
                    
                    // Security validation for result size
                    if (resultValue.length() > MAX_RESULT_LENGTH) {
                        LOGGER.warn("JSONPath result too large ({} characters), max allowed is {}: {}", 
                            resultValue.length(), MAX_RESULT_LENGTH, resultValue.substring(0, 100) + "...");
                        resultValue = resultValue.substring(0, MAX_RESULT_LENGTH);
                    }
                    
                    // Generate attribute name
                    String attributeName = "jsonpath_" + jsonPathExpression.replaceAll("[^a-zA-Z0-9_]", "_");
                    
                    // Security validation for attribute name length
                    if (attributeName.length() > MAX_ATTRIBUTE_NAME_LENGTH) {
                        LOGGER.warn("Attribute name too long ({} characters), truncating to {}: {}", 
                            attributeName.length(), MAX_ATTRIBUTE_NAME_LENGTH, attributeName);
                        attributeName = attributeName.substring(0, MAX_ATTRIBUTE_NAME_LENGTH);
                    }
                    
                    // Add the result to attributes using the exchange's withAttribute method
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
            // Security validation for expression length
            if (jsonPathExpression.length() > MAX_TEMPLATE_LENGTH) {
                LOGGER.warn("JSONPath expression too long ({} characters), max allowed is {}: {}", 
                    jsonPathExpression.length(), MAX_TEMPLATE_LENGTH, jsonPathExpression.substring(0, 100) + "...");
                return null;
            }
            
            // Convert the Exchange to a JSON structure that JSONPath can navigate
            Object exchangeData = createExchangeMap(exchange);
            
            LOGGER.debug("Exchange data for JSONPath: {}", exchangeData);
            
            // Parse and evaluate the JSONPath expression with timeout protection
            Future<Object> future = jsonPathExecutor.submit(() -> {
                JsonPath jsonPath = JsonPath.compile(jsonPathExpression);
                return jsonPath.read(exchangeData, JSON_PATH_CONFIG);
            });
            
            Object result;
            try {
                result = future.get(JSONPATH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                LOGGER.debug("JSONPath '{}' evaluated to: {}", jsonPathExpression, result);
            } catch (TimeoutException e) {
                LOGGER.warn("JSONPath evaluation timed out after {}ms for expression: {}", 
                    JSONPATH_TIMEOUT_MS, jsonPathExpression);
                future.cancel(true);
                return null;
            } catch (InterruptedException e) {
                LOGGER.warn("JSONPath evaluation interrupted for expression: {}", jsonPathExpression);
                Thread.currentThread().interrupt();
                future.cancel(true);
                return null;
            } catch (ExecutionException e) {
                LOGGER.debug("JSONPath evaluation failed for '{}': {}", jsonPathExpression, e.getMessage());
                return null;
            }
            
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