package io.github.clescot.kafka.connect.http.core.template.resolver;

import io.github.clescot.kafka.connect.http.core.Exchange;

import java.util.Optional;

/**
 * Interface for template expressions that can be resolved by processors.
 * Each expression has a processor name, an expression string, and optionally a separator for arrays.
 */
public interface TemplateExpression {

    /**
     * Get the name of the processor that should handle this expression.
     * @return the processor name (e.g., "jsonpath", "math", "datetime")
     */
    String getProcessorName();

    /**
     * Get the expression string to be processed.
     * @return the expression (e.g., "$.price", "100 * 1.2", "now:yyyy-MM-dd")
     */
    String getExpression();

    /**
     * Get the separator for array/collection results.
     * @return Optional containing the separator string, or empty if not applicable
     */
    Optional<String> getSeparator();

    /**
     * Get the raw expression as it appears in the template.
     * @return the raw expression including delimiters (e.g., "${jsonpath:$.price}")
     */
    String getRawExpression();

    /**
     * Resolve this expression using the given exchange.
     * @param exchange the exchange containing data to process
     * @return the resolved value as a string
     */
    String resolve(Exchange<?, ?> exchange);
}
