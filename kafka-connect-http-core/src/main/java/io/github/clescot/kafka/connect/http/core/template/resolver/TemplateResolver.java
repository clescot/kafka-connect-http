package io.github.clescot.kafka.connect.http.core.template.resolver;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import io.github.clescot.kafka.connect.http.core.template.ExchangeTemplateProcessor;
import io.github.clescot.kafka.connect.http.core.template.ExchangeTemplateProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Resolver for template expressions.
 * Handles recursive resolution of nested expressions, resolving children first
 * and then parent expressions.
 */
public class TemplateResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateResolver.class);

    private final ExpressionParser expressionParser;
    private final ExchangeTemplateProcessorFactory processorFactory;

    public TemplateResolver() {
        this.expressionParser = new ExpressionParser();
        this.processorFactory = new ExchangeTemplateProcessorFactory();
    }

    /**
     * Resolve a template string against an exchange.
     * Expressions are resolved recursively, with children resolved before parents.
     *
     * @param exchange the exchange containing data to process
     * @param template the template string to resolve
     * @param context additional context for template processing
     * @return the resolved template string
     */
    public <R extends Request, S extends Response> String resolve(
            Exchange<R, S> exchange,
            String template,
            Map<String, Object> context) {

        if (template == null || template.isEmpty()) {
            return template;
        }

        LOGGER.debug("Resolving template: {}", template);

        List<ExpressionNode> expressions = expressionParser.parse(template);

        if (expressions.isEmpty()) {
            LOGGER.debug("No expressions found in template");
            return template;
        }

        String result = template;

        for (ExpressionNode node : expressions) {
            String resolvedValue = resolveNode(exchange, node, context);
            result = result.replace(node.rawExpression(), resolvedValue);
            LOGGER.debug("Replaced '{}' with '{}'", node.rawExpression(), resolvedValue);
        }

        LOGGER.debug("Resolved template: {}", result);
        return result;
    }

    /**
     * Resolve a single expression node, including any nested children.
     *
     * @param exchange the exchange containing data
     * @param node the expression node to resolve
     * @param context additional context
     * @return the resolved value
     */
    private String resolveNode(Exchange<?, ?> exchange, ExpressionNode node, Map<String, Object> context) {
        String expressionToResolve = node.expression();

        if (node.hasChildren()) {
            LOGGER.trace("Resolving {} children for expression '{}'", node.children().size(), node.rawExpression());

            for (ExpressionNode child : node.children()) {
                String childResolved = resolveNode(exchange, child, context);
                expressionToResolve = expressionToResolve.replace(child.rawExpression(), childResolved);
                LOGGER.trace("Child '{}' resolved to '{}'", child.rawExpression(), childResolved);
            }
        }

        return resolveWithProcessor(exchange, node, expressionToResolve, context);
    }

    /**
     * Resolve the expression using the appropriate processor.
     *
     * @param exchange the exchange containing data
     * @param node the expression node
     * @param resolvedExpression the expression with children already resolved
     * @param context additional context
     * @return the resolved value
     */
    private String resolveWithProcessor(
            Exchange<?, ?> exchange,
            ExpressionNode node,
            String resolvedExpression,
            Map<String, Object> context) {

        String processorName = node.processorName();

        ExchangeTemplateProcessor processor;
        try {
            processor = processorFactory.createBuiltinProcessor(processorName);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("No processor found for '{}'. Keeping original expression.", processorName);
            return node.rawExpression();
        }

        String processorExpression = resolvedExpression;

        if (node.hasSeparator()) {
            processorExpression = resolvedExpression + ":" + node.getSeparatorOrDefault();
        }

        String template = "${" + processorName + ":" + processorExpression + "}";

        try {
            Exchange<?, ?> processedExchange = processor.process(exchange, template, context);
            return processedExchange.getContent();
        } catch (Exception e) {
            LOGGER.warn("Failed to resolve expression '{}': {}. Keeping original expression.",
                    node.rawExpression(), e.getMessage());
            return node.rawExpression();
        }
    }
}
