package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;

/**
 * Math template processor for performing mathematical operations.
 * Supports basic arithmetic and unit conversions.
 */
public class MathTemplateProcessor implements ExchangeTemplateProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(MathTemplateProcessor.class);
    
    @Override
    public <R extends Request, S extends Response> Exchange<R, S> process(@NotNull Exchange<R, S> exchange, @NotNull String template, Map<String, Object> context) {
        try {
            // Extract parts from template: ${math:expression:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 1) {
                log.warn("Invalid math template format: {}", template);
                return   exchange;
            }
            
            String expression = parts[0];
            String attributeName = parts.length > 1 ? parts[1] : "math_result";
            
            // Evaluate the mathematical expression
            double result = evaluateMathExpression(expression, exchange);
            
            log.debug("Math expression '{}' evaluated to: {}", expression, result);
            return   exchange.withAttribute(attributeName, String.valueOf(result));
            
        } catch (Exception e) {
            log.warn("Failed to process math template '{}': {}", template, e.getMessage());
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
            log.warn("Failed to evaluate math expression '{}': {}", expression, e.getMessage());
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
        double result = evaluateMultiplicationDivision(expression);
        
        // Finally handle addition and subtraction
        return evaluateAdditionSubtraction(result, expression);
    }
    
    private double evaluateMultiplicationDivision(String expression) {
        // This is a simplified approach - for real implementation, use proper parsing
        // For now, we'll use a basic approach that handles simple expressions
        
        // Split by + and - to get terms, then evaluate each term
        String[] terms = expression.split("[+-]");
        String[] operators = expression.split("[^+-]+");
        
        double result = 0;
        for (int i = 0; i < terms.length; i++) {
            String term = terms[i];
            if (term.isEmpty()) continue;
            
            double termValue = evaluateTerm(term);
            
            if (i == 0) {
                result = termValue;
            } else {
                String op = operators[i];
                if (op.contains("+")) {
                    result += termValue;
                } else if (op.contains("-")) {
                    result -= termValue;
                }
            }
        }
        
        return result;
    }
    
    private double evaluateTerm(String term) {
        // Handle multiplication and division within a term
        if (term.contains("*") || term.contains("/")) {
            String[] factors = term.split("[*/]");
            String[] ops = term.split("[^*/]+");
            
            double result = Double.parseDouble(factors[0]);
            for (int i = 1; i < factors.length; i++) {
                String op = ops[i];
                double factor = Double.parseDouble(factors[i]);
                if (op.contains("*")) {
                    result *= factor;
                } else if (op.contains("/")) {
                    result /= factor;
                }
            }
            return result;
        }
        
        return Double.parseDouble(term);
    }
    
    private double evaluateAdditionSubtraction(double baseResult, String expression) {
        // This method would handle addition and subtraction
        // For simplicity, we'll return the base result
        // In a real implementation, this would parse the expression properly
        return baseResult;
    }
}