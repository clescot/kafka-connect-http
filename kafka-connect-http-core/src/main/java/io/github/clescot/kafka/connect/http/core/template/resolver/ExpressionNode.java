package io.github.clescot.kafka.connect.http.core.template.resolver;

import java.util.List;
import java.util.Optional;

/**
 * Record representing a parsed template expression node.
 * Contains the expression data and any nested child expressions.
 */
public record ExpressionNode(
        String rawExpression,
        String processorName,
        String expression,
        Optional<String> separator,
        List<ExpressionNode> children,
        int nestingDepth
) {
    /**
     * Check if this node has nested child expressions.
     * @return true if children list is not empty
     */
    public boolean hasChildren() {
        return children != null && !children.isEmpty();
    }

    /**
     * Check if this node represents an array/collection result with a separator.
     * @return true if a separator is defined
     */
    public boolean hasSeparator() {
        return separator != null && separator.isPresent();
    }

    /**
     * Get the separator or return default comma if not defined.
     * @return the separator string, or ", " by default
     */
    public String getSeparatorOrDefault() {
        return separator.orElse(", ");
    }
}
