package io.github.clescot.kafka.connect.http.core.template;

import java.util.List;

/**
 * Represents a composite node that contains multiple child nodes.
 * Used when a template contains multiple separate expressions.
 */
public class CompositeNode implements ExpressionNode {
    private final List<ExpressionNode> children;

    public CompositeNode(List<ExpressionNode> children) {
        this.children = children;
    }

    public List<ExpressionNode> getChildren() {
        return children;
    }

    @Override
    public boolean containsTemplates() {
        for (ExpressionNode child : children) {
            if (child.containsTemplates()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ExpressionNode child : children) {
            sb.append(child.toString());
        }
        return sb.toString();
    }
}
