package io.github.clescot.kafka.connect.http.core.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility class for creating and configuring template managers for any configuration type.
 * Provides shared template processing functionality for HttpConfiguration, SseConfiguration, etc.
 */
public class TemplateConfigurationUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateConfigurationUtil.class);

    private TemplateConfigurationUtil() {}

    /**
     * Creates a template manager configured with the given settings.
     * This method provides a consistent way to create template managers across different configuration types.
     *
     * @param settings the configuration settings
     * @return configured ExchangeTemplateManager
     */
    public static ExchangeTemplateManager createTemplateManager(Map<String, String> settings) {
        ExchangeTemplateManager manager = new ExchangeTemplateManager();
        
        // Register built-in processors
        manager.registerDefaultProcessors();
        
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
     * Gets the exchange template from settings.
     *
     * @param settings the configuration settings
     * @return the exchange template, or empty string if not configured
     */
    public static String getExchangeTemplate(Map<String, String> settings) {
        return settings.getOrDefault("exchange.template", "");
    }

    /**
     * Sets the exchange template in settings.
     *
     * @param settings the configuration settings
     * @param template the template to set
     */
    public static void setExchangeTemplate(Map<String, String> settings, String template) {
        settings.put("exchange.template", template);
    }
}