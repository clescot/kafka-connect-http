package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Math template processor for performing mathematical operations.
 * Supports basic arithmetic operations.
 */
public class MathTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MathTemplateProcessor.class);
    public static final String NAME = "math";

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> Exchange<R, S> process(
            @NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            String expression = extractExpression(template);
            if (expression == null || expression.isEmpty()) {
                LOGGER.warn("Invalid math template format: {}", template);
                return exchange;
            }

            LOGGER.debug("Evaluating math expression: {}", expression);

            double result = evaluateSimpleExpression(expression);

            String resultValue = String.valueOf(result);
            LOGGER.debug("Math expression '{}' evaluated to: {}", expression, resultValue);

            return setContent(exchange, resultValue);

        } catch (Exception e) {
            LOGGER.warn("Failed to process math template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }

    private String extractExpression(String template) {
        if (!template.startsWith("${math:") || !template.endsWith("}")) {
            return null;
        }
        return template.substring("${math:".length(), template.length() - 1);
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

    private double evaluateSimpleExpression(String expression) {
        expression = expression.replaceAll("\\s+", "");

        while (expression.contains("(") && expression.contains(")")) {
            int openParen = expression.lastIndexOf('(');
            int closeParen = expression.indexOf(')', openParen);
            if (closeParen > openParen) {
                String subExpression = expression.substring(openParen + 1, closeParen);
                double subResult = evaluateSimpleExpression(subExpression);
                expression = expression.substring(0, openParen) + subResult + expression.substring(closeParen + 1);
            }
        }

        return evaluateMultiplicationDivision(expression);
    }

    private double evaluateMultiplicationDivision(String expression) {
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

    @Override
    public boolean supports(@NotNull String template) {
        return template != null && template.startsWith("${math:") && template.endsWith("}");
    }

    @Override
    public String getName() {
        return NAME;
    }
}
