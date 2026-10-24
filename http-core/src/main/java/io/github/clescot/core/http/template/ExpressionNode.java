package io.github.clescot.core.http.template;

/**
 * Base interface for all expression nodes in the template AST.
 */
public interface ExpressionNode {

    /**
     * Check if this node contains any template expressions.
     *
     * @return true if this node contains templates
     */
    boolean containsTemplates();

    /**
     * Get the string representation of this node.
     *
     * @return the string representation
     */
    String toString();
}
