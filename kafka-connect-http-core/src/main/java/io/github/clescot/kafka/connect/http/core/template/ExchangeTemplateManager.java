package io.github.clescot.kafka.connect.http.core.template;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manager for Exchange template processors.
 * Handles registration, lookup, and processing of templates using appropriate processors.
 * Works with any Exchange implementation (HttpExchange, SseExchange, etc.).
 */
public class ExchangeTemplateManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeTemplateManager.class);

    private final Map<String, ExchangeTemplateProcessor> processors = new ConcurrentHashMap<>();
    private final List<ExchangeTemplateProcessor> processorList = new ArrayList<>();
    private Pattern combinedPattern;
    /**
     * Default constructor that registers all default processors.
     */
    public ExchangeTemplateManager() {
        this(true);
    }

    /**
     * Constructor that allows specifying whether to register default processors.
     *
     * @param registerDefaults true to register default processors, false otherwise
     */
    public ExchangeTemplateManager(boolean registerDefaults) {
        if (registerDefaults) {
            registerDefaultProcessors();
        }
    }

    /**
     * Register a template processor.
     * 
     * @param processor the processor to register
     */
    public void registerProcessor(ExchangeTemplateProcessor processor) {
        if (processor == null) {
            throw new IllegalArgumentException("Processor cannot be null");
        }
        String name = processor.getName();
        if (processors.containsKey(name)) {
            LOGGER.warn("Processor with name '{}' already registered, overwriting", name);
        }
        processors.put(name, processor);
        processorList.add(processor);
        LOGGER.info("Registered ExchangeTemplateProcessor: {}", name);
        combinedPattern = buildPattern(processorList);
    }

    /**
     * Unregister a template processor by name.
     * 
     * @param name the name of the processor to unregister
     */
    public void unregisterProcessor(String name) {
        ExchangeTemplateProcessor removed = processors.remove(name);
        if (removed != null) {
            processorList.remove(removed);
            LOGGER.info("Unregistered ExchangeTemplateProcessor: {}", name);
        }
    }

    /**
     * Get a processor by name.
     * 
     * @param name the name of the processor
     * @return the processor, or null if not found
     */
    public ExchangeTemplateProcessor getProcessor(String name) {
        return processors.get(name);
    }



    /**
     * Resolve a template string against an exchange.
     * This method extracts and resolves template expressions in the given template string.
     * It properly handles templates with multiple expression types by applying the appropriate
     * processor for each expression and preserving the template structure.
     *
     * @param exchange the Exchange containing data for template resolution
     * @param template the template string to resolve
     * @param context additional context for template processing
     * @return the resolved template string, or the original template if no processing occurred
     */
    public <R extends Request, S extends Response> String resolveTemplate(
            @NotNull Exchange<R, S> exchange,
            @NotNull String template,
            Map<String, Object> context) {

        if (template == null || template.isEmpty()) {
            return template;
        }

        if (processorList.isEmpty()) {
            LOGGER.warn("No template processors registered, returning original template");
            return template;
        }


        Matcher matcher = combinedPattern.matcher(template);

        StringBuilder result = new StringBuilder();
        int lastEnd = 0;

        while (matcher.find()) {
            // Append text before this match
            result.append(template, lastEnd, matcher.start());

            // Get the full match (e.g., "${jsonpath:$.response.statusCode}")
            String fullMatch = matcher.group(0);

            // Find which processor can handle this expression
            String resolvedValue = null;
            for (ExchangeTemplateProcessor processor : processorList) {
                if (processor.supports(fullMatch)) {
                    // Process the expression
                    Exchange<R, S> processedExchange = processor.process(exchange, fullMatch, context);
                    resolvedValue = processedExchange.getContent();

                    // If the processor returned something different from the expression, use it
                    if (resolvedValue != null && !resolvedValue.equals(fullMatch)) {
                        break;
                    }
                    resolvedValue = null;
                }
            }

            if (resolvedValue != null) {
                result.append(resolvedValue);
            } else {
                // If no processor could resolve it, keep the original
                result.append(fullMatch);
            }

            lastEnd = matcher.end();
        }

        // Append remaining text after last match
        result.append(template, lastEnd, template.length());

        String resolved = result.toString();
        LOGGER.debug("Resolved template '{}' to '{}'", template, resolved);
        return resolved;
    }



    private static @NonNull Pattern buildPattern(List<ExchangeTemplateProcessor> processors) {
        Preconditions.checkArgument(!processors.isEmpty());
        StringBuilder patternBuilder = new StringBuilder("\\$\\{(?:");
        for (ExchangeTemplateProcessor processor : processors) {
            patternBuilder.append(processor.getTemplatePattern()).append("|");
        }
        patternBuilder.setLength(patternBuilder.length() - 1);
        patternBuilder.append(")\\}");

        return Pattern.compile(patternBuilder.toString());
    }



    /**
     * Get all registered processors.
     * 
     * @return unmodifiable collection of all processors
     */
    public Collection<ExchangeTemplateProcessor> getProcessors() {
        return Collections.unmodifiableCollection(processors.values());
    }

    /**
     * Register default processors.
     */
    public void registerDefaultProcessors() {
        // Register existing processors
        registerProcessor(new JsonPathExchangeTemplateProcessor());
        registerProcessor(new XPathExchangeTemplateProcessor());
        registerProcessor(new RandomExchangeTemplateProcessor());
        registerProcessor(new JmesPathExchangeTemplateProcessor());
        registerProcessor(new RegexExchangeTemplateProcessor());
        registerProcessor(new HeaderParameterTemplateProcessor());
        registerProcessor(new DateTimeTemplateProcessor());
        registerProcessor(new ConditionalTemplateProcessor());
        registerProcessor(new HashingTemplateProcessor());
        registerProcessor(new MathTemplateProcessor());
    }

    /**
     * Clear all registered processors.
     */
    public void clearProcessors() {
        processors.clear();
        processorList.clear();
        LOGGER.info("Cleared all ExchangeTemplateProcessors");
    }

    /**
     * Check if any processor can handle the given template.
     * 
     * @param template the template to check
     * @return true if at least one processor can handle the template
     */
    public boolean canProcess(String template) {
        return processorList.stream().anyMatch(processor -> processor.supports(template));
    }
}