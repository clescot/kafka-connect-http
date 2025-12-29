package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager for Exchange template processors.
 * Handles registration, lookup, and processing of templates using appropriate processors.
 * Works with any Exchange implementation (HttpExchange, SseExchange, etc.).
 */
public class ExchangeTemplateManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeTemplateManager.class);
    
    private final Map<String, ExchangeTemplateProcessor> processors = new ConcurrentHashMap<>();
    private final List<ExchangeTemplateProcessor> processorList = new ArrayList<>();

    /**
     * Default constructor that registers all default processors.
     */
    public ExchangeTemplateManager() {
        registerDefaultProcessors();
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
     * Process a template using the appropriate processor.
     * 
     * @param exchange the Exchange to process
     * @param template the template to process
     * @param context additional context for processing
     * @return the processed Exchange
     * @throws IllegalStateException if no processor can handle the template
     */
    public <R extends Request,S extends Response> Exchange<R, S> processTemplate(@NotNull Exchange<R, S> exchange, @NotNull String template, Map<String, Object> context) {
        if (processorList.isEmpty()) {
            LOGGER.warn("No template processors registered, returning original exchange");
            return exchange;
        }

        Exchange<R, S> result = exchange;
        boolean processed = false;

        // Apply all processors that support this template
        for (ExchangeTemplateProcessor processor : processorList) {
            if (processor.supports(template)) {
                LOGGER.debug("Applying processor '{}' for template processing", processor.getName());
                result = processor.process(result, template, context);
                processed = true;
            }
        }

        if (!processed) {
            LOGGER.warn("No processor found for template: {}", template);
        }

        return result;
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
        
        // Register new processors
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