package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Interface for processing Exchange templates with plugins.
 * Allows customization of Exchange output based on templates and plugin processing.
 * Works with any Exchange implementation (HttpExchange, SseExchange, etc.).
 */
public interface ExchangeTemplateProcessor {

    /**
     * Process the Exchange using the given template and context.
     * 
     * @param exchange the original Exchange to process
     * @param template the template string that may contain plugin expressions
     * @param context additional context data for template processing
     * @return the processed Exchange with customized output
     */
    <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context);

    /**
     * Get the name/identifier of this processor.
     * 
     * @return the processor name
     */
    String getName();

    /**
     * Check if this processor can handle the given template.
     * 
     * @param template the template to check
     * @return true if this processor can handle the template, false otherwise
     */
    boolean supports(String template);
}