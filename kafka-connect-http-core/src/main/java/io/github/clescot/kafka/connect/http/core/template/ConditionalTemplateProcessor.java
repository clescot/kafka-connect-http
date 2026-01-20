package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Conditional template processor for implementing logic in templates.
 * Supports if-then-else conditions.
 */
public class ConditionalTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionalTemplateProcessor.class);
    public static final String NAME = "conditional";

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> Exchange<R, S> process(
            @NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            String[] parts = extractTemplateParts(template);
            if (parts.length < 3) {
                LOGGER.warn("Invalid conditional template format: {}", template);
                return exchange;
            }

            String condition = parts[0];
            String trueValue;
            String falseValue;

            if (condition.equalsIgnoreCase("has") && parts.length > 3) {
                trueValue = parts[2];
                falseValue = parts[3];
                condition = "has:" + parts[1];
            } else if (condition.equalsIgnoreCase("status") && parts.length > 3) {
                trueValue = parts[2];
                falseValue = parts[3];
                condition = "status:" + parts[1];
            } else if (condition.equalsIgnoreCase("status")) {
                trueValue = parts[2];
                falseValue = parts[1];
                condition = "status:" + parts[1];
            } else {
                trueValue = parts[1];
                falseValue = parts[2];
            }

            System.out.println("DEBUG: template=" + template + ", parts.length=" + parts.length);
            System.out.println("DEBUG: condition='" + condition + "', trueValue='" + trueValue + "', falseValue='" + falseValue + "'");
            
            boolean conditionResult = evaluateCondition(condition, exchange);
            String result = conditionResult ? trueValue : falseValue;

            LOGGER.debug("Condition '{}' evaluated to {}, result: {}", condition, conditionResult, result);
            return setContent(exchange, result);

        } catch (Exception e) {
            LOGGER.warn("Failed to process conditional template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }

    @Override
    public boolean supports(@NotNull String template) {
        return template != null && template.startsWith("${if:") && template.contains(":");
    }

    @Override
    public String getTemplatePattern() {
        return "if:[^}]+";
    }

    @Override
    public String getName() {
        return NAME;
    }

    private String[] extractTemplateParts(String template) {
        if (!template.startsWith("${if:") || !template.endsWith("}")) {
            return new String[0];
        }
        String innerContent = template.substring("${if:".length(), template.length() - 1);
        int firstColon = innerContent.indexOf(':');
        if (firstColon <= 0) return new String[]{innerContent};

        String prefix = innerContent.substring(0, firstColon);
        int secondColon;

        if (prefix.equalsIgnoreCase("status")) {
            int firstColonAfterStatus = innerContent.indexOf(':', firstColon + 1);
            int lastColon = innerContent.lastIndexOf(':');
            if (firstColonAfterStatus < lastColon) {
                secondColon = innerContent.indexOf(':', firstColon + 1);
            } else {
                secondColon = lastColon;
            }
        } else {
            secondColon = innerContent.indexOf(':', firstColon + 1);
        }

        if (secondColon <= firstColon) return new String[]{innerContent.substring(0, firstColon), innerContent.substring(firstColon + 1)};

        String condition = innerContent.substring(0, firstColon);
        String trueValue = innerContent.substring(firstColon + 1, secondColon);
        String remaining = innerContent.substring(secondColon + 1);
        int thirdColon = remaining.indexOf(':');

        if (thirdColon > 0) {
            String falseValue = remaining.substring(0, thirdColon);
            String attributeName = remaining.substring(thirdColon + 1);
            return new String[]{condition, trueValue, falseValue, attributeName};
        } else {
            return new String[]{condition, trueValue, remaining};
        }
    }

    private boolean evaluateCondition(String condition, Exchange<?, ?> exchange) {
        try {
            if (condition.equalsIgnoreCase("true") || condition.equalsIgnoreCase("yes")) return true;
            if (condition.equalsIgnoreCase("false") || condition.equalsIgnoreCase("no")) return false;
            if (condition.startsWith("has:")) {
                String attrName = condition.substring(4);
                Object attrValue = exchange.getAttribute(attrName);
                return attrValue != null && !attrValue.toString().isEmpty();
            }
            if (condition.startsWith("status:")) return evaluateStatusCondition(condition.substring(7), exchange);
            if (condition.contains(">") || condition.contains("<") || condition.contains("=") || condition.contains("!")) {
                return evaluateComparison(condition, exchange);
            }
            if (condition.startsWith("contains:") || condition.startsWith("matches:")) {
                return evaluateStringCondition(condition, exchange);
            }
            Object attrValue = exchange.getAttribute(condition);
            return attrValue != null && !attrValue.toString().isEmpty();
        } catch (Exception e) {
            LOGGER.warn("Failed to evaluate condition '{}': {}", condition, e.getMessage());
            return false;
        }
    }

    private boolean evaluateStatusCondition(String statusCondition, Exchange<?, ?> exchange) {
        if (!(exchange instanceof HttpExchange)) return false;
        HttpExchange httpExchange = (HttpExchange) exchange;
        if (httpExchange.getResponse() == null) return false;
        int statusCode = httpExchange.getResponse().getStatusCode();
        if (statusCondition.contains(">=")) return statusCode >= Integer.parseInt(statusCondition.substring(2));
        if (statusCondition.contains("<=")) return statusCode <= Integer.parseInt(statusCondition.substring(2));
        if (statusCondition.contains(">")) return statusCode > Integer.parseInt(statusCondition.substring(1));
        if (statusCondition.contains("<")) return statusCode < Integer.parseInt(statusCondition.substring(1));
        if (statusCondition.contains("==")) return statusCode == Integer.parseInt(statusCondition.substring(2));
        if (statusCondition.contains("!=")) return statusCode != Integer.parseInt(statusCondition.substring(2));
        if (statusCondition.contains("-")) {
            String[] range = statusCondition.split("-");
            return statusCode >= Integer.parseInt(range[0]) && statusCode <= Integer.parseInt(range[1]);
        }
        return false;
    }

    private boolean evaluateComparison(String condition, Exchange<?, ?> exchange) {
        String operator = "";
        if (condition.contains(">=")) operator = ">=";
        else if (condition.contains("<=")) operator = "<=";
        else if (condition.contains("!=")) operator = "!=";
        else if (condition.contains(">")) operator = ">";
        else if (condition.contains("<")) operator = "<";
        else if (condition.contains("==")) operator = "==";
        if (operator.isEmpty()) return false;
        String[] parts = condition.split(Pattern.quote(operator), 2);
        String left = parts[0].trim();
        String right = parts[1].trim();
        double leftValue = left.matches("\\d+(\\.\\d+)?") ? Double.parseDouble(left) : getNumericAttribute(left, exchange);
        double rightValue = right.matches("\\d+(\\.\\d+)?") ? Double.parseDouble(right) : getNumericAttribute(right, exchange);
        switch (operator) {
            case ">=": return leftValue >= rightValue;
            case "<=": return leftValue <= rightValue;
            case "!=": return leftValue != rightValue;
            case ">": return leftValue > rightValue;
            case "<": return leftValue < rightValue;
            case "==": return leftValue == rightValue;
            default: return false;
        }
    }

    private double getNumericAttribute(String attrName, Exchange<?, ?> exchange) {
        Object attr = exchange.getAttribute(attrName);
        if (attr == null) return 0;
        try { return Double.parseDouble(attr.toString()); } catch (NumberFormatException e) { return 0; }
    }

    private boolean evaluateStringCondition(String condition, Exchange<?, ?> exchange) {
        if (condition.startsWith("contains:")) {
            String[] parts = condition.substring(9).split(":", 2);
            String attrName = parts[0];
            String substring = parts.length > 1 ? parts[1] : "";
            Object attr = exchange.getAttribute(attrName);
            return attr != null && attr.toString().contains(substring);
        } else if (condition.startsWith("matches:")) {
            String[] parts = condition.substring(8).split(":", 2);
            String attrName = parts[0];
            String regex = parts.length > 1 ? parts[1] : "";
            Object attr = exchange.getAttribute(attrName);
            return attr != null && attr.toString().matches(regex);
        }
        return false;
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
}
