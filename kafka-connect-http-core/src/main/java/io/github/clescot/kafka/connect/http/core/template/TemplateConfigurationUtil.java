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
        registerDefaultProcessors(manager);
        
        // Check for custom processor configuration
        String customProcessors = settings.getOrDefault("exchange.template.processors", "");
        if (!customProcessors.trim().isEmpty()) {
            String[] processorNames = customProcessors.split(",");
            for (String processorName : processorNames) {
                try {
                    ExchangeTemplateProcessor processor = new ExchangeTemplateProcessorFactory().createBuiltinProcessor(processorName.trim());
                    manager.registerProcessor(processor);
                    LOGGER.info("Registered custom processor: {}", processorName.trim());
                } catch (Exception e) {
                    LOGGER.warn("Failed to load custom processor '{}': {}", processorName.trim(), e.getMessage());
                }
            }
        }
        
        return manager;
    }

    /**
     * Registers default processors to the given template manager.
     * This ensures consistent default processor registration across different configuration types.
     *
     * @param manager the template manager to register processors to
     */
    public static void registerDefaultProcessors(ExchangeTemplateManager manager) {
        // Register existing processors
        manager.registerProcessor(new JsonPathExchangeTemplateProcessor());
        manager.registerProcessor(new XPathExchangeTemplateProcessor());
        manager.registerProcessor(new RandomExchangeTemplateProcessor());
        
        // Register new processors
        manager.registerProcessor(new JmesPathExchangeTemplateProcessor());
        manager.registerProcessor(new RegexExchangeTemplateProcessor());
        manager.registerProcessor(new HeaderParameterTemplateProcessor());
        manager.registerProcessor(new DateTimeTemplateProcessor());
        manager.registerProcessor(new ConditionalTemplateProcessor());
        manager.registerProcessor(new HashingTemplateProcessor());
        manager.registerProcessor(new MathTemplateProcessor());
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