package io.github.clescot.kafka.connect.http.core.template;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JSONPath template processor for Exchange.
 * Allows extracting and transforming data from any Exchange implementation using JSONPath expressions.
 */
public class JsonPathExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathExchangeTemplateProcessor.class);

    private static final long JSONPATH_TIMEOUT_MS = 5000;
    private static final int MAX_TEMPLATE_LENGTH = 1000;
    private static final int MAX_RESULT_LENGTH = 10000;

    private static final ExecutorService jsonPathExecutor = Executors.newCachedThreadPool();

    public static final String NAME = "jsonpath";
    private static final Pattern JSONPATH_PATTERN = Pattern.compile("\\$\\{jsonpath:(.*?)\\}");

    private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.SUPPRESS_EXCEPTIONS, Option.DEFAULT_PATH_LEAF_TO_NULL)
            .build();

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        LOGGER.debug("Processing template with JSONPath: {}", template);

        if (template.length() > MAX_TEMPLATE_LENGTH) {
            LOGGER.warn("Template too long ({} characters), max allowed is {}: {}",
                    template.length(), MAX_TEMPLATE_LENGTH, template.substring(0, 100) + "...");
            return exchange;
        }

        Matcher matcher = JSONPATH_PATTERN.matcher(template);
        if (!matcher.find()) {
            LOGGER.debug("No JSONPath expression found in template: {}", template);
            return exchange;
        }

        StringBuilder resultBuilder = new StringBuilder();
        boolean firstMatch = true;
        String currentTemplate = template;

        matcher.reset();
        while (matcher.find()) {
            String fullMatch = matcher.group(0);
            String jsonPathExpression = matcher.group(1);

            String separator = null;
            if (jsonPathExpression.contains(":")) {
                int lastColonIndex = jsonPathExpression.lastIndexOf(':');
                String potentialSeparator = jsonPathExpression.substring(lastColonIndex + 1);
                if (potentialSeparator.startsWith("'") || potentialSeparator.startsWith("\"")) {
                    if (potentialSeparator.length() >= 2 &&
                            (potentialSeparator.endsWith("'") || potentialSeparator.endsWith("\""))) {
                        separator = potentialSeparator.substring(1, potentialSeparator.length() - 1);
                        jsonPathExpression = jsonPathExpression.substring(0, lastColonIndex);
                        LOGGER.debug("Extracted separator: '{}', expression: '{}'", separator, jsonPathExpression);
                    }
                }
            }

            try {
                Object result = evaluateJsonPath(exchange, jsonPathExpression);

                if (result != null) {
                    String resultValue = formatResult(result, separator);

                    if (resultValue.length() > MAX_RESULT_LENGTH) {
                        LOGGER.warn("JSONPath result too large ({} characters), max allowed is {}: {}",
                                resultValue.length(), MAX_RESULT_LENGTH, resultValue.substring(0, 100) + "...");
                        resultValue = resultValue.substring(0, MAX_RESULT_LENGTH);
                    }

                    LOGGER.debug("JSONPath expression '{}' resolved to: {}", jsonPathExpression, resultValue);

                    if (!firstMatch) {
                        resultBuilder.append(" ");
                    }
                    resultBuilder.append(resultValue);
                    firstMatch = false;
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to evaluate JSONPath expression '{}': {}", jsonPathExpression, e.getMessage());
                LOGGER.debug("Exception details:", e);
            }
        }

        if (resultBuilder.length() == 0) {
            LOGGER.debug("No JSONPath expressions could be evaluated");
            return exchange;
        }

        String resultValue = resultBuilder.toString();
        LOGGER.debug("JSONPath resolved to: {}", resultValue);

        return setContent(exchange, resultValue);
    }

    private String formatResult(Object result, String separator) {
        if (result instanceof List) {
            List<?> list = (List<?>) result;
            if (list.isEmpty()) {
                return "";
            }
            String sep = separator != null ? separator : ", ";
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    sb.append(sep);
                }
                Object item = list.get(i);
                if (item instanceof com.fasterxml.jackson.databind.JsonNode jsonNode) {
                    sb.append(jsonNode.isTextual() ? jsonNode.asText() : jsonNode.toString());
                } else {
                    sb.append(item.toString());
                }
            }
            return sb.toString();
        } else if (result instanceof com.fasterxml.jackson.databind.JsonNode jsonNode) {
            if (jsonNode.isTextual()) {
                return jsonNode.asText();
            } else if (jsonNode.isNumber()) {
                return jsonNode.asText();
            } else if (jsonNode.isBoolean()) {
                return jsonNode.asText();
            } else {
                return jsonNode.toString();
            }
        } else {
            return result.toString();
        }
    }

    @SuppressWarnings("unchecked")
    private <R extends Request, S extends Response> Exchange<R, S> setContent(
            Exchange<R, S> exchange, String content) {

        if (exchange instanceof HttpExchange) {
            HttpExchange httpExchange = (HttpExchange) exchange;
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

    private Object evaluateJsonPath(Exchange<?, ?> exchange, String jsonPathExpression) {
        try {
            if (jsonPathExpression.length() > MAX_TEMPLATE_LENGTH) {
                LOGGER.warn("JSONPath expression too long ({} characters), max allowed is {}: {}",
                        jsonPathExpression.length(), MAX_TEMPLATE_LENGTH, jsonPathExpression.substring(0, 100) + "...");
                return null;
            }

            Object exchangeData = createExchangeMap(exchange);

            LOGGER.debug("Exchange data for JSONPath: {}", exchangeData);

            Future<Object> future = jsonPathExecutor.submit(() -> {
                JsonPath jsonPath = JsonPath.compile(jsonPathExpression);
                return jsonPath.read(exchangeData, JSON_PATH_CONFIG);
            });

            try {
                Object result = future.get(JSONPATH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                LOGGER.debug("JSONPath '{}' evaluated to: {}", jsonPathExpression, result);
                return result;
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

        } catch (Exception e) {
            LOGGER.debug("JSONPath evaluation failed for '{}': {}", jsonPathExpression, e.getMessage());
            LOGGER.debug("Exception details:", e);
            return null;
        }
    }

    private Object createExchangeMap(Exchange<?, ?> exchange) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode exchangeNode = objectMapper.createObjectNode();

            Map<String, Object> metadata = exchange.getMetadata();
            for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                exchangeNode.set(entry.getKey(), objectMapper.valueToTree(entry.getValue()));
            }

            exchangeNode.put("content", exchange.getContent());

            exchangeNode.set("attributes", objectMapper.valueToTree(exchange.getAttributes()));

            return exchangeNode;

        } catch (Exception e) {
            LOGGER.warn("Failed to create JSON exchange map: {}", e.getMessage());
            Map<String, Object> exchangeMap = new HashMap<>();

            exchangeMap.putAll(exchange.getMetadata());

            exchangeMap.put("content", exchange.getContent());

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
