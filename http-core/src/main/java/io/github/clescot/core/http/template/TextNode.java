package io.github.clescot.core.http.template;

/**
 * Represents literal text in a template (not containing any template expressions).
 */
public class TextNode implements ExpressionNode {
    private final String text;

    public TextNode(String text) {
        this.text = text != null ? text : "";
    }

    @Override
    public boolean containsTemplates() {
        return text != null && text.contains("${");
    }

    @Override
    public String toString() {
        return text;
    }

    public String getText() {
        return text;
    }
}
