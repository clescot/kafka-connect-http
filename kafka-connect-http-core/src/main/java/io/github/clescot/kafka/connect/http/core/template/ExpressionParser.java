package io.github.clescot.kafka.connect.http.core.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses template strings into an Abstract Syntax Tree (AST) of ExpressionNodes.
 * Uses a stack-based algorithm to handle nested braces correctly.
 */
public class ExpressionParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionParser.class);
    private static final int MAX_DEPTH = 50;

    /**
     * Parse a template string into an AST.
     */
    public ExpressionNode parse(String template) {
        if (template == null || template.isEmpty()) {
            return new TextNode(template);
        }

        List<ExpressionNode> rootChildren = new ArrayList<>();
        parseTemplate(template, rootChildren, 0);

        if (rootChildren.size() == 1 && rootChildren.get(0) instanceof TemplateNode) {
            return rootChildren.get(0);
        }

        return new CompositeNode(rootChildren);
    }

    /**
     * Recursive parsing method that builds nodes.
     */
    private void parseTemplate(String template, List<ExpressionNode> children, int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException(
                    "Template nesting depth exceeded maximum (" + MAX_DEPTH + "): " + truncate(template, 100));
        }

        int currentIndex = 0;
        int braceDepth = 0;
        StringBuilder currentContent = new StringBuilder();

        while (currentIndex < template.length()) {
            char c = template.charAt(currentIndex);
            char nextChar = currentIndex + 1 < template.length() ? template.charAt(currentIndex + 1) : '\0';

            if (c == '$' && nextChar == '{') {
                if (braceDepth == 0) {
                    if (currentContent.length() > 0) {
                        children.add(new TextNode(currentContent.toString()));
                        currentContent.setLength(0);
                    }
                    braceDepth = 1;
                    currentContent.append("${");  // Add ${ to content
                    currentIndex += 2;
                    continue;
                }
            }

            if (braceDepth > 0) {
                if (c == '{') {
                    braceDepth++;
                } else if (c == '}') {
                    braceDepth--;
                }
                currentContent.append(c);
                if (braceDepth == 0) {
                    String fullTemplate = currentContent.toString();
                    ExpressionNode node = parseTemplateExpression(fullTemplate);
                    children.add(node);
                    currentContent.setLength(0);
                }
            } else {
                currentContent.append(c);
            }

            currentIndex++;
        }

        if (currentContent.length() > 0) {
            String remaining = currentContent.toString();
            if (braceDepth > 0) {
                children.add(new TextNode(remaining));
                LOGGER.warn("Unclosed template expression found: {}", truncate(remaining, 50));
            } else {
                children.add(new TextNode(remaining));
            }
        }
    }

    /**
     * Parse a complete ${...} expression.
     */
    private ExpressionNode parseTemplateExpression(String fullTemplate) {
        if (!fullTemplate.startsWith("${") || !fullTemplate.endsWith("}")) {
            return new TextNode(fullTemplate);
        }

        String innerContent = fullTemplate.substring(2, fullTemplate.length() - 1);
        if (innerContent.isEmpty()) {
            return new TextNode(fullTemplate);
        }

        int firstColon = innerContent.indexOf(':');
        String processorName;
        String content;

        if (firstColon > 0) {
            // Extract processor name - take everything before the first colon
            // But strip any dot notation (e.g., "random.int" -> "random")
            String rawProcessorName = innerContent.substring(0, firstColon);
            int dotIndex = rawProcessorName.indexOf('.');
            if (dotIndex > 0) {
                processorName = rawProcessorName.substring(0, dotIndex);
            } else {
                processorName = rawProcessorName;
            }
            content = innerContent.substring(firstColon + 1);
        } else {
            processorName = innerContent;
            content = "";
        }

        TemplateNode templateNode = new TemplateNode(processorName, content);

        if (content != null && content.contains("${")) {
            List<ExpressionNode> nestedChildren = new ArrayList<>();
            parseTemplate(content, nestedChildren, 1);
            for (ExpressionNode child : nestedChildren) {
                templateNode.addChild(child);
            }
        }

        return templateNode;
    }

    private String truncate(String s, int maxLength) {
        if (s == null) return null;
        return s.length() > maxLength ? s.substring(0, maxLength) + "..." : s;
    }
}
