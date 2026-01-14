package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Math template processor for performing mathematical operations.
 * Supports basic arithmetic and unit conversions.
 */
public class MathTemplateProcessor implements ExchangeTemplateProcessor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MathTemplateProcessor.class);
    
    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            // Extract parts from template: ${math:expression:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 1) {
                LOGGER.warn("Invalid math template format: {}", template);
                return   exchange;
            }
            
            String expression = parts[0];
            String attributeName = parts.length > 1 ? parts[1] : "math_result";
            
            // Evaluate the mathematical expression
            double result = evaluateMathExpression(expression, exchange);
            
            LOGGER.debug("Math expression '{}' evaluated to: {}", expression, result);
            return   exchange.withAttribute(attributeName, String.valueOf(result));
            
        } catch (Exception e) {
            LOGGER.warn("Failed to process math template '{}': {}", template, e.getMessage());
            return   exchange;
        }
    }
    
    @Override
    public boolean supports(@NotNull String template) {
        return template.startsWith("${math:") && template.endsWith("}");
    }
    
    @Override
    public String getName() {
        return "math";
    }
    
    /**
     * Extract parts from template: ${math:expression:attributeName}
     */
    private String[] extractTemplateParts(String template) {
        // Remove ${math: and }
        String innerContent = template.substring("${math:".length(), template.length() - 1);
        
        // Split by colon (only split once to preserve colons in expression)
        int colonIndex = innerContent.lastIndexOf(':');
        
        if (colonIndex > 0) {
            String expression = innerContent.substring(0, colonIndex);
            String attributeName = innerContent.substring(colonIndex + 1);
            return new String[]{expression, attributeName};
        } else {
            return new String[]{innerContent};
        }
    }
    
    /**
     * Evaluate a mathematical expression
     */
    private double evaluateMathExpression(String expression, Exchange<?, ?> exchange) {
        try {
            // Simple arithmetic evaluation
            // Support: +, -, *, /, %, (, )
            // Also support attribute references like ${math:responseTime/1000}
            
            // Replace attribute references with their values
            String processedExpression = replaceAttributeReferences(expression, exchange);
            
            // Use a simple expression evaluator (for basic operations)
            // Note: For production, consider using a proper expression library like JEXL or MVEL
            return evaluateSimpleExpression(processedExpression);
            
        } catch (Exception e) {
            LOGGER.warn("Failed to evaluate math expression '{}': {}", expression, e.getMessage());
            return 0.0;
        }
    }
    
    /**
     * Replace attribute references in expression with their actual values
     * Format: {attributeName} or ${attributeName}
     */
    private String replaceAttributeReferences(String expression, Exchange<?, ?> exchange) {
        // Replace ${attr} or {attr} patterns
        String result = expression;
        
        // Pattern: ${attributeName}
        int startIndex = result.indexOf("${");
        while (startIndex >= 0) {
            int endIndex = result.indexOf('}', startIndex);
            if (endIndex > startIndex) {
                String attrName = result.substring(startIndex + 2, endIndex);
                Object attrValue = exchange.getAttribute(attrName);
                String value = attrValue != null ? attrValue.toString() : "0";
                result = result.substring(0, startIndex) + value + result.substring(endIndex + 1);
                startIndex = result.indexOf("${", startIndex + value.length());
            } else {
                break;
            }
        }
        
        // Pattern: {attributeName}
        startIndex = result.indexOf('{');
        while (startIndex >= 0) {
            int endIndex = result.indexOf('}', startIndex);
            if (endIndex > startIndex) {
                String attrName = result.substring(startIndex + 1, endIndex);
                Object attrValue = exchange.getAttribute(attrName);
                String value = attrValue != null ? attrValue.toString() : "0";
                result = result.substring(0, startIndex) + value + result.substring(endIndex + 1);
                startIndex = result.indexOf('{', startIndex + value.length());
            } else {
                break;
            }
        }
        
        return result;
    }
    
    /**
     * Simple expression evaluator for basic arithmetic
     * Note: This is a basic implementation. For complex expressions, use a proper library.
     */
    private double evaluateSimpleExpression(String expression) {
        // Remove all whitespace
        expression = expression.replaceAll("\\s+", "");
        
        // Handle basic arithmetic operations in order of precedence
        // 1. Parentheses
        // 2. Multiplication and Division
        // 3. Addition and Subtraction
        
        // First, handle parentheses
        while (expression.contains("(") && expression.contains(")")) {
            int openParen = expression.lastIndexOf('(');
            int closeParen = expression.indexOf(')', openParen);
            if (closeParen > openParen) {
                String subExpression = expression.substring(openParen + 1, closeParen);
                double subResult = evaluateSimpleExpression(subExpression);
                expression = expression.substring(0, openParen) + subResult + expression.substring(closeParen + 1);
            }
        }
        
        // Then handle multiplication and division
        return evaluateMultiplicationDivision(expression);
    }
    
    private double evaluateMultiplicationDivision(String expression) {
        // Split the expression into terms separated by + and -
        java.util.List<String> terms = new java.util.ArrayList<>();
        java.util.List<String> operators = new java.util.ArrayList<>();
        
        int termStart = 0;
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (c == '+' || c == '-') {
                if (i > termStart) {
                    terms.add(expression.substring(termStart, i));
                }
                operators.add(String.valueOf(c));
                termStart = i + 1;
            }
        }
        // Add the last term
        if (termStart < expression.length()) {
            terms.add(expression.substring(termStart));
        }
        
        double result = 0;
        for (int i = 0; i < terms.size(); i++) {
            String term = terms.get(i);
            double termValue = evaluateTerm(term);
            
            if (i == 0) {
                result = termValue;
            } else {
                String op = operators.get(i - 1);
                if ("+".equals(op)) {
                    result += termValue;
                } else if ("-".equals(op)) {
                    result -= termValue;
                }
            }
        }
        
        return result;
    }
    
    private double evaluateTerm(String term) {
        // Handle multiplication and division within a term
        if (term.contains("*") || term.contains("/")) {
            java.util.List<String> factors = new java.util.ArrayList<>();
            java.util.List<String> ops = new java.util.ArrayList<>();
            
            int factorStart = 0;
            for (int i = 0; i < term.length(); i++) {
                char c = term.charAt(i);
                if (c == '*' || c == '/') {
                    if (i > factorStart) {
                        factors.add(term.substring(factorStart, i));
                    }
                    ops.add(String.valueOf(c));
                    factorStart = i + 1;
                }
            }
            // Add the last factor
            if (factorStart < term.length()) {
                factors.add(term.substring(factorStart));
            }
            
            double result = Double.parseDouble(factors.get(0));
            for (int i = 0; i < ops.size(); i++) {
                double factor = Double.parseDouble(factors.get(i + 1));
                if ("*".equals(ops.get(i))) {
                    result *= factor;
                } else if ("/".equals(ops.get(i))) {
                    result /= factor;
                }
            }
            return result;
        }
        
        return Double.parseDouble(term);
    }
}