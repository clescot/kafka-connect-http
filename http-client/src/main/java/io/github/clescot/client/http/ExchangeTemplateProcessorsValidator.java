package io.github.clescot.client.http;

import io.github.clescot.core.http.template.ExchangeTemplateProcessorFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.stream.Collectors;

/**
 * Validates the 'exchange.template.processors' setting.
 * <p>
 * Each comma-separated entry must be the name of a built-in processor, or of a processor
 * discovered via the Java Service Loader. An empty value is valid and means "all built-in
 * processors are enabled". Unknown names are rejected, so that typos fail at connector
 * configuration validation instead of silently producing unresolved templates.
 */
public class ExchangeTemplateProcessorsValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            return;
        }
        String processorNames = value.toString().trim();
        if (processorNames.isEmpty()) {
            return;
        }
        for (String processorName : processorNames.split(",")) {
            String trimmed = processorName.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (!ExchangeTemplateProcessorFactory.isKnownProcessorName(trimmed)) {
                String knownProcessors = ExchangeTemplateProcessorFactory.builtinProcessorNames().stream()
                        .sorted()
                        .collect(Collectors.joining(", "));
                throw new ConfigException(
                        name,
                        value,
                        "'" + trimmed + "' is not a known template processor. built-in processors: [" + knownProcessors
                                + "]. custom processors must be provided via the Java Service Loader and can be listed by name.");
            }
        }
    }

    @Override
    public String toString() {
        return "a comma-separated list of template processor names (empty means all built-in processors)";
    }
}
