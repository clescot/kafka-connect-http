package io.github.clescot.kafka.connect.http.core.template;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a template expression like ${jsonpath:...} or ${math:...}.
 * The content may contain nested templates that need to be evaluated first.
 */
public class TemplateNode implements ExpressionNode {
    private final String processorName;
    private final String rawContent;
    private final List<ExpressionNode> children;

    public TemplateNode(String processorName, String rawContent) {
        this.processorName = processorName != null ? processorName : "";
        this.rawContent = rawContent != null ? rawContent : "";
        this.children = new ArrayList<>();
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getRawContent() {
        return rawContent;
    }

    public void addChild(ExpressionNode child) {
        if (child != null) {
            children.add(child);
        }
    }

    public List<ExpressionNode> getChildren() {
        return new ArrayList<>(children);
    }

    @Override
    public boolean containsTemplates() {
        return !children.isEmpty() || rawContent.contains("${");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("${");
        sb.append(processorName);
        if (!processorName.isEmpty() && !rawContent.isEmpty()) {
            sb.append(":");
        }
        sb.append(rawContent);
        sb.append("}");
        return sb.toString();
    }

    /**
     * Get the full template expression string.
     *
     * @return the full ${...} expression
     */
    public String getFullTemplate() {
        return toString();
    }
}
