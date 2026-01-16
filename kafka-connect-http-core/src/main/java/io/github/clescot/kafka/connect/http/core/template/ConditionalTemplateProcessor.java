package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Conditional template processor for implementing logic in templates.
 * Supports if-then-else conditions and basic comparisons.
 */
public class ConditionalTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionalTemplateProcessor.class);
    public static final String NAME = "conditional";

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            String[] parts = extractTemplateParts(template);
            if (parts.length < 3) {
                LOGGER.warn("Invalid conditional template format: {}", template);
                return   exchange;
            }
            
            String condition = parts[0];
            String trueValue = parts[1];
            String falseValue = parts[2];
            String attributeName = parts.length > 3 ? parts[3] : "conditional_result";
            
            boolean conditionResult = evaluateCondition(condition, exchange);
            String result = conditionResult ? trueValue : falseValue;
            
            LOGGER.debug("Condition '{}' evaluated to {}, result: {}", condition, conditionResult, result);
            return   exchange.withAttribute(attributeName, result);
            
        } catch (Exception e) {
            LOGGER.warn("Failed to process conditional template '{}': {}", template, e.getMessage());
            return   exchange;
        }
    }
    
    @Override
    public boolean supports(@NotNull String template) {
        return template.startsWith("${if:") && template.contains(":");
    }
    
    @Override
    public String getName() {
        return NAME;
    }
    
    private String[] extractTemplateParts(String template) {
        String innerContent = template.substring("${if:".length(), template.length() - 1);
        int firstColon = innerContent.indexOf(':');
        if (firstColon <= 0) return new String[]{innerContent};
        int secondColon = innerContent.indexOf(':', firstColon + 1);
        if (secondColon <= 0) return new String[]{innerContent.substring(0, firstColon), innerContent.substring(firstColon + 1)};
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
            if (condition.startsWith("has:")) return   exchange.getAttribute(condition.substring(4)) != null;
            if (condition.startsWith("status:")) return evaluateStatusCondition(condition.substring(7), exchange);
            if (condition.contains(">") || condition.contains("<") || condition.contains("=") || condition.contains("!")) return evaluateComparison(condition, exchange);
            if (condition.startsWith("contains:") || condition.startsWith("matches:")) return evaluateStringCondition(condition, exchange);
            Object attrValue = exchange.getAttribute(condition);
            return attrValue != null && !attrValue.toString().isEmpty();
        } catch (Exception e) {
            LOGGER.warn("Failed to evaluate condition '{}': {}", condition, e.getMessage());
            return false;
        }
    }
    
    private boolean evaluateStatusCondition(String statusCondition, Exchange<?, ?> exchange) {
        if (!(exchange instanceof io.github.clescot.kafka.connect.http.core.HttpExchange)) return false;
        io.github.clescot.kafka.connect.http.core.HttpExchange httpExchange = (io.github.clescot.kafka.connect.http.core.HttpExchange) exchange;
        if (httpExchange.getResponse() == null) return false;
        int statusCode = httpExchange.getResponse().getStatusCode();
        if (statusCondition.contains(">=")) return statusCode >= Integer.parseInt(statusCondition.substring(2));
        if (statusCondition.contains("<=")) return statusCode <= Integer.parseInt(statusCondition.substring(2));
        if (statusCondition.contains(">")) return statusCode > Integer.parseInt(statusCondition.substring(1));
        if (statusCondition.contains("<")) return statusCode < Integer.parseInt(statusCondition.substring(1));
        if (statusCondition.contains("==")) return statusCode == Integer.parseInt(statusCondition.substring(2));
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
}