package io.github.clescot.kafka.connect.http.core.template.resolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for template expressions.
 * Identifies and parses template expressions from a template string,
 * handling nested expressions recursively.
 */
public class ExpressionParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionParser.class);

    private static final int MAX_NESTING_DEPTH = 10;

    private static final Pattern EXPRESSION_PATTERN = Pattern.compile(
            "\\$\\{([a-zA-Z]+)(?:\\.\\w+)?:([^{}]+)\\}"
    );

    private static final Pattern SEPARATOR_PATTERN = Pattern.compile(
            "^(.+):(['\"])(.*)\\2$"
    );

    /**
     * Parse a template string and extract all expressions.
     * Handles nested expressions by parsing from the inside out.
     *
     * @param template the template string to parse
     * @return list of ExpressionNode representing the top-level expressions
     * @throws IllegalArgumentException if nesting depth exceeds MAX_NESTING_DEPTH
     */
    public List<ExpressionNode> parse(String template) {
        return extractTopLevelExpressions(template);
    }

    /**
     * Recursively parse expressions from the template.
     *
     * @param template the template string to parse
     * @param currentDepth the current nesting depth
     * @param results the list to add parsed expressions to
     * @throws IllegalArgumentException if nesting depth exceeds MAX_NESTING_DEPTH
     */
    private void parseExpressions(String template, int currentDepth, List<ExpressionNode> results) {
        if (currentDepth > MAX_NESTING_DEPTH) {
            String errorMsg = String.format(
                    "Template nesting depth exceeded maximum %d. Template: %s",
                    MAX_NESTING_DEPTH, template.substring(0, Math.min(100, template.length()))
            );
            LOGGER.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        Matcher matcher = EXPRESSION_PATTERN.matcher(template);
        int lastEnd = 0;

        while (matcher.find()) {
            String fullMatch = matcher.group(0);
            String processorName = matcher.group(1);
            String expression = matcher.group(2);

            int matchStart = matcher.start();
            int matchEnd = matcher.end();

            if (matchStart > lastEnd) {
                LOGGER.trace("Skipping non-expression text: '{}'",
                        template.substring(lastEnd, matchStart));
            }

            Optional<String> separator = parseSeparator(expression);
            String cleanExpression = separator.isPresent()
                    ? expression.substring(0, expression.lastIndexOf(':'))
                    : expression;

            List<ExpressionNode> children = new ArrayList<>();
            boolean hasNested = containsNestedExpression(cleanExpression);

            if (hasNested) {
                LOGGER.trace("Found nested expression in '{}': {}", fullMatch, cleanExpression);
                parseExpressions(cleanExpression, currentDepth + 1, children);
            }

            ExpressionNode node = new ExpressionNode(
                    fullMatch,
                    processorName,
                    cleanExpression,
                    separator,
                    children,
                    currentDepth
            );

            results.add(node);
            LOGGER.trace("Parsed expression: processor='{}', expression='{}', children={}",
                    processorName, cleanExpression, children.size());

            lastEnd = matchEnd;
        }

        if (lastEnd < template.length()) {
            LOGGER.trace("Skipping trailing non-expression text: '{}'",
                    template.substring(lastEnd));
        }
    }

    /**
     * Extract only the top-level expressions from the template.
     * For nested expressions, only the outermost expression is returned,
     * with children available in the ExpressionNode.
     *
     * @param template the template string to parse
     * @return list of top-level ExpressionNode
     */
    public List<ExpressionNode> extractTopLevelExpressions(String template) {
        if (template == null || template.isEmpty()) {
            return List.of();
        }

        List<ExpressionNode> topLevelExpressions = new ArrayList<>();
        parseExpressions(template, 0, topLevelExpressions);
        return topLevelExpressions;
    }

    /**
     * Check if the expression contains nested template expressions.
     *
     * @param expression the expression to check
     * @return true if nested expressions are found
     */
    private boolean containsNestedExpression(String expression) {
        return EXPRESSION_PATTERN.matcher(expression).find();
    }

    /**
     * Parse the separator from the expression if present.
     * Separators are specified at the end of the expression with quotes.
     * Format: ${processor:expression:','} or ${processor:expression:" | "}
     *
     * @param expression the expression to parse
     * @return Optional containing the separator, or empty if not present
     */
    private Optional<String> parseSeparator(String expression) {
        Matcher separatorMatcher = SEPARATOR_PATTERN.matcher(expression);
        if (separatorMatcher.find()) {
            String separator = separatorMatcher.group(3);
            LOGGER.trace("Found separator: '{}' in expression '{}'", separator, expression);
            return Optional.of(separator);
        }
        return Optional.empty();
    }
}
