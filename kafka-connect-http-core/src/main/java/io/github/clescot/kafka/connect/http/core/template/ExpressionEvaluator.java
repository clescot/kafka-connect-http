package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Evaluates an AST of ExpressionNodes by traversing it bottom-up.
 * Ensures that nested templates are evaluated before their parent templates.
 *
 * @param <R> the request type
 * @param <S> the response type
 */
public class ExpressionEvaluator<R extends Request, S extends Response> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionEvaluator.class);

    private final Exchange<R, S> exchange;
    private final Map<String, Object> context;
    private final ExpressionTemplateManager templateManager;
    private int evaluationDepth = 0;
    private static final int MAX_EVALUATION_DEPTH = 100;

    public ExpressionEvaluator(
            @NotNull Exchange<R, S> exchange,
            Map<String, Object> context,
            ExpressionTemplateManager templateManager) {
        this.exchange = exchange;
        this.context = context != null ? context : Map.of();
        this.templateManager = templateManager;
    }

    /**
     * Evaluate an expression node and return the result as a string.
     *
     * @param node the node to evaluate
     * @return the evaluated string result
     */
    public String evaluate(ExpressionNode node) {
        if (node == null) {
            return "";
        }

        if (evaluationDepth > MAX_EVALUATION_DEPTH) {
            LOGGER.warn("Evaluation depth exceeded maximum ({}), returning node as string", MAX_EVALUATION_DEPTH);
            return node.toString();
        }

        evaluationDepth++;

        try {
            if (node instanceof TextNode textNode) {
                return evaluateTextNode(textNode);
            } else if (node instanceof TemplateNode templateNode) {
                return evaluateTemplateNode(templateNode);
            } else if (node instanceof CompositeNode compositeNode) {
                return evaluateCompositeNode(compositeNode);
            } else {
                return node.toString();
            }
        } finally {
            evaluationDepth--;
        }
    }

    /**
     * Evaluate a text node. If it contains nested templates, evaluate those first.
     */
    private String evaluateTextNode(TextNode textNode) {
        String text = textNode.getText();
        if (!textNode.containsTemplates()) {
            return text;
        }

        // Text contains nested templates - need to re-parse and evaluate
        ExpressionParser parser = new ExpressionParser();
        ExpressionNode parsed = parser.parse(text);
        return evaluate(parsed);
    }

    /**
     * Evaluate a composite node by evaluating all its children and concatenating the results.
     */
    private String evaluateCompositeNode(CompositeNode compositeNode) {
        StringBuilder sb = new StringBuilder();
        for (ExpressionNode child : compositeNode.getChildren()) {
            sb.append(evaluate(child));
        }
        return sb.toString();
    }

    /**
     * Evaluate a template node by:
     * 1. Evaluating all children first (bottom-up)
     * 2. Building the resolved content string
     * 3. Delegating to the appropriate processor
     */
    private String evaluateTemplateNode(TemplateNode templateNode) {
        String processorName = templateNode.getProcessorName();
        String rawContent = templateNode.getRawContent();
        List<ExpressionNode> children = templateNode.getChildren();
        String fullTemplate = templateNode.getFullTemplate();

        String resolvedContent;
        String templateToProcess;

        if (children.isEmpty()) {
            // No nested templates - use raw content directly
            resolvedContent = rawContent;
            templateToProcess = fullTemplate;
        } else {
            // Build the content string by evaluating all children
            StringBuilder contentBuilder = new StringBuilder();
            for (ExpressionNode child : children) {
                String childResult = evaluate(child);
                contentBuilder.append(childResult);
            }
            resolvedContent = contentBuilder.toString();
            templateToProcess = buildTemplate(processorName, resolvedContent);
        }

        // Find the processor and delegate
        ExchangeTemplateProcessor processor = templateManager.getProcessor(processorName);
        if (processor == null) {
            LOGGER.debug("No processor found for '{}', returning original template", processorName);
            return fullTemplate;
        }

        try {
            // Use the original template format for processing
            Exchange<R, S> result = processor.process(exchange, templateToProcess, context);
            String resultContent = result.getContent();

            if (resultContent == null) {
                return fullTemplate;
            }

            if (resultContent.equals(fullTemplate)) {
                return fullTemplate;
            }

            if (containsTemplatePattern(resultContent)) {
                ExpressionParser parser = new ExpressionParser();
                ExpressionNode parsedResult = parser.parse(resultContent);
                return evaluate(parsedResult);
            }

            return resultContent;

        } catch (Exception e) {
            LOGGER.warn("Failed to process template '{}' with processor '{}': {}",
                    fullTemplate, processorName, e.getMessage());
            return fullTemplate;
        }
    }

    /**
     * Build a template string from processor name and content.
     */
    private String buildTemplate(String processorName, String content) {
        StringBuilder sb = new StringBuilder("${");
        sb.append(processorName);
        if (!content.isEmpty()) {
            sb.append(":");
            sb.append(content);
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Check if a string contains template patterns.
     */
    private boolean containsTemplatePattern(String s) {
        return s != null && s.contains("${");
    }

    /**
     * Interface for accessing processors from the evaluator.
     */
    public interface ExpressionTemplateManager {
        ExchangeTemplateProcessor getProcessor(String name);
    }
}
