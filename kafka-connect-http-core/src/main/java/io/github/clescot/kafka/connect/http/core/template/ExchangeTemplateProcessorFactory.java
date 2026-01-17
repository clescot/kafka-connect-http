package io.github.clescot.kafka.connect.http.core.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory for creating ExchangeTemplateProcessor instances.
 * Supports both built-in processors and service loader discovery.
 */
public class ExchangeTemplateProcessorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeTemplateProcessorFactory.class);

    /**
     * Create a new instance of a built-in template processor.
     * 
     * @param processorName the name of the processor to create
     * @return the created processor instance
     * @throws IllegalArgumentException if the processor name is unknown
     */
    public ExchangeTemplateProcessor createBuiltinProcessor(String processorName) {
        if (processorName == null || processorName.trim().isEmpty()) {
            throw new IllegalArgumentException("Processor name cannot be null or empty");
        }

        String normalizedName = processorName.trim().toLowerCase();

        return switch (normalizedName) {
            case JsonPathExchangeTemplateProcessor.NAME -> new JsonPathExchangeTemplateProcessor();
            case XPathExchangeTemplateProcessor.NAME -> new XPathExchangeTemplateProcessor();
            case RandomExchangeTemplateProcessor.NAME -> new RandomExchangeTemplateProcessor();
            case JmesPathExchangeTemplateProcessor.NAME -> new JmesPathExchangeTemplateProcessor();
            case RegexExchangeTemplateProcessor.NAME -> new RegexExchangeTemplateProcessor();
            case HeaderParameterTemplateProcessor.NAME -> new HeaderParameterTemplateProcessor();
            case DateTimeTemplateProcessor.NAME -> new DateTimeTemplateProcessor();
            case ConditionalTemplateProcessor.NAME -> new ConditionalTemplateProcessor();
            case HashingTemplateProcessor.NAME -> new HashingTemplateProcessor();
            case MathTemplateProcessor.NAME -> new MathTemplateProcessor();
            default -> throw new IllegalArgumentException("Unknown built-in processor: " + processorName);
        };
    }

    /**
     * Discover and load template processors using Java Service Loader.
     * 
     * @return ExchangeTemplateManager with all discovered processors registered
     */
    public ExchangeTemplateManager discoverProcessors() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager();
        
        try {
            ServiceLoader<ExchangeTemplateProcessor> loader = ServiceLoader.load(ExchangeTemplateProcessor.class);
            for (ExchangeTemplateProcessor processor : loader) {
                manager.registerProcessor(processor);
                LOGGER.info("Discovered and registered template processor: {}", processor.getName());
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to discover template processors via ServiceLoader: {}", e.getMessage());
        }
        
        return manager;
    }

    public ExchangeTemplateManager createTemplateManager(Map<String,String> settings){
        ExchangeTemplateManager manager = new ExchangeTemplateManager();

        // Check for custom processor configuration
        String customProcessors = settings.getOrDefault("exchange.template.processors", "");
        if (!customProcessors.trim().isEmpty()) {
            String[] processorNames = customProcessors.split(",");
            for (String processorName : processorNames) {
                try {
                    ExchangeTemplateProcessor processor = new ExchangeTemplateProcessorFactory().createBuiltinProcessor(processorName.trim());
                    manager.registerProcessor(processor);
                    if(LOGGER.isInfoEnabled()) {
                        LOGGER.info("Registered custom processor: {}", processorName.trim());
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to load custom processor '{}': {}", processorName.trim(), e.getMessage());
                }
            }
        }
        return manager;
    }

    /**
     * Create a template manager with default built-in processors.
     * 
     * @return ExchangeTemplateManager with default processors registered
     */
    public ExchangeTemplateManager createDefaultTemplateManager() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager();
        
        // Register built-in processors
        manager.registerDefaultProcessors();

        // Discover additional processors via ServiceLoader
        ExchangeTemplateManager discoveredManager = discoverProcessors();
        for (ExchangeTemplateProcessor processor : discoveredManager.getProcessors()) {
            // Avoid duplicate registration
            if (!manager.getProcessors().stream().anyMatch(p -> p.getName().equals(processor.getName()))) {
                manager.registerProcessor(processor);
            }
        }
        
        return manager;
    }
}