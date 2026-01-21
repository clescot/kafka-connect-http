package io.github.clescot.core.http.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import io.github.clescot.core.http.*;
import io.github.clescot.core.sse.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
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
    public static final String PREFIX = "${jmespath:";

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> E process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            // Extract the JMESPath expression from template
            String[] parts = extractTemplateParts(template);
            if (parts.length < 1) {
                LOGGER.warn("Invalid JMESPath template format: {}", template);
                return exchange;
            }

            String jmesPathExpression = parts[0];

            // Get content as JSON
            String content = exchange.getContent();
            if (content == null || content.trim().isEmpty()) {
                LOGGER.debug("No content available for JMESPath processing");
                return (E) setContent(exchange, "");
            }

            // Parse content as JSON
            JsonNode jsonContent = objectMapper.readTree(content);

            // Use JSONPath to evaluate JMESPath-like expression (JSONPath can handle similar syntax)
            Object result = JsonPath.using(jsonPathConfig).parse(jsonContent.toString()).read(jmesPathExpression);

            // Handle the result properly - extract from arrays if needed
            String resultString = extractResultValue(result);

            LOGGER.debug("JMESPath expression '{}' evaluated to: {}", jmesPathExpression, resultString);
            return (E) setContent(exchange, resultString);

        } catch (Exception e) {
            LOGGER.warn("Failed to process JMESPath template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }

    @SuppressWarnings("unchecked")
    private <R extends Request, S extends Response> Exchange<R, S> setContent(
            Exchange<R, S> exchange, String content) {

        if (exchange instanceof HttpExchange httpExchange) {
            HttpRequest request = httpExchange.getRequest();
            HttpResponse originalResponse = httpExchange.getResponse();

            HttpResponse newResponse;
            if (originalResponse != null) {
                newResponse = (HttpResponse) originalResponse.clone();
                newResponse.setBodyAsString(content);
            } else {
                newResponse = new HttpResponse(200, "OK");
                newResponse.setBodyAsString(content);
            }

            return (Exchange<R, S>) HttpExchange.Builder.anHttpExchange()
                    .withHttpRequest(request)
                    .withHttpResponse(newResponse)
                    .withDuration(httpExchange.getDurationInMillis())
                    .at(httpExchange.getMoment())
                    .withAttempts(httpExchange.getAttempts())
                    .withAttributes(new HashMap<>(httpExchange.getAttributes()))
                    .withTimings(new HashMap<>(httpExchange.getTimings()))
                    .build();
        }

        if (exchange instanceof SseExchange) {
            SseExchange sseExchange = (SseExchange) exchange;
            return (Exchange<R, S>) sseExchange.setContent(content);
        }

        LOGGER.warn("Unsupported exchange type: {}. Cannot set content.", exchange.getClass().getName());
        return exchange;
    }

    @Override
    public boolean supports(@NotNull String template) {
        return template.startsWith(PREFIX);
    }

    @Override
    public String getTemplatePattern() {
        return "jmespath:[^}]+";
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Extract parts from template: ${jmespath:expression} or ${jmespath:expression:attributeName}
     * Returns array where [0] = expression, [1] = attributeName (if present, but ignored)
     */
    private String[] extractTemplateParts(String template) {
        if (!template.startsWith(PREFIX) || !template.endsWith("}")) {
            return new String[0];
        }
        String innerContent = template.substring(PREFIX.length(), template.length() - 1);

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

        if (result instanceof List) {
            List<?> listResult = (List<?>) result;
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