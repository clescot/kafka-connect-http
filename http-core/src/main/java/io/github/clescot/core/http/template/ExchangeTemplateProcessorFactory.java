package io.github.clescot.core.http.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Factory for creating ExchangeTemplateProcessor instances.
 * Supports both built-in processors and service loader discovery.
 */
public class ExchangeTemplateProcessorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeTemplateProcessorFactory.class);

    /**
     * Settings key used to restrict the set of enabled template processors.
     * When absent or empty, all built-in processors are enabled.
     */
    public static final String EXCHANGE_TEMPLATE_PROCESSORS_KEY = "exchange.template.processors";

    /**
     * Names (and documented aliases) of all the built-in processors.
     */
    private static final Set<String> BUILTIN_PROCESSOR_NAMES = Set.of(
            JsonPathExchangeTemplateProcessor.NAME,
            XPathExchangeTemplateProcessor.NAME,
            RandomExchangeTemplateProcessor.NAME,
            JmesPathExchangeTemplateProcessor.NAME,
            RegexExchangeTemplateProcessor.NAME,
            HeaderParameterTemplateProcessor.NAME,
            "headerparam",
            DateTimeTemplateProcessor.NAME,
            ConditionalTemplateProcessor.NAME,
            "conditional",
            HashingTemplateProcessor.NAME,
            MathTemplateProcessor.NAME);

    /**
     * Get the names (and documented aliases) of all the built-in processors.
     *
     * @return an unmodifiable set of built-in processor names
     */
    public static Set<String> builtinProcessorNames() {
        return BUILTIN_PROCESSOR_NAMES;
    }

    /**
     * Check if the given name denotes a built-in processor, aliases included. The check is case-insensitive.
     *
     * @param processorName the processor name to check
     * @return true if the name denotes a built-in processor
     */
    public static boolean isBuiltinProcessorName(String processorName) {
        return processorName != null && BUILTIN_PROCESSOR_NAMES.contains(processorName.trim().toLowerCase());
    }

    /**
     * Check if the given name can be resolved to a processor: either a built-in processor
     * or a processor discovered via the Java Service Loader.
     *
     * @param processorName the processor name to check
     * @return true if the name denotes a known processor
     */
    public static boolean isKnownProcessorName(String processorName) {
        if (isBuiltinProcessorName(processorName)) {
            return true;
        }
        String name = processorName == null ? "" : processorName.trim();
        if (name.isEmpty()) {
            return false;
        }
        return discoverProcessorsByName().containsKey(name);
    }

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
            case HeaderParameterTemplateProcessor.NAME, "headerparam" -> new HeaderParameterTemplateProcessor();
            case DateTimeTemplateProcessor.NAME -> new DateTimeTemplateProcessor();
            case ConditionalTemplateProcessor.NAME, "conditional" -> new ConditionalTemplateProcessor();
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

    /**
     * Create a template manager from settings.
     * <p>
     * When {@link #EXCHANGE_TEMPLATE_PROCESSORS_KEY} is absent or blank, every built-in processor is
     * registered, plus the ones discovered via the Java Service Loader. When it is set to a
     * comma-separated list of processor names, only the listed processors are registered:
     * the list is a whitelist, not a set of additions.
     *
     * @param settings the connector settings
     * @return a template manager configured from the settings
     */
    public ExchangeTemplateManager createTemplateManager(Map<String,String> settings){
        String customProcessors = settings == null ? null : settings.get(EXCHANGE_TEMPLATE_PROCESSORS_KEY);

        // No explicit selection -> every built-in processor is enabled (plus discovered ones).
        if (customProcessors == null || customProcessors.trim().isEmpty()) {
            return createDefaultTemplateManager();
        }

        // Explicit selection -> only the listed processors are registered.
        ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
        // index ServiceLoader-discovered processors by name, so that custom processors can be whitelisted too.
        Map<String, ExchangeTemplateProcessor> discoveredProcessors = discoverProcessorsByName();
        for (String processorName : customProcessors.split(",")) {
            String trimmed = processorName.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            ExchangeTemplateProcessor processor = tryCreateBuiltinProcessor(trimmed)
                    .orElseGet(() -> discoveredProcessors.get(trimmed));
            if (processor == null) {
                LOGGER.warn("Unknown template processor '{}' ignored. Built-in processors: {}", trimmed, BUILTIN_PROCESSOR_NAMES);
                continue;
            }
            manager.registerProcessor(processor);
            if(LOGGER.isInfoEnabled()) {
                LOGGER.info("Registered template processor: {}", trimmed);
            }
        }
        return manager;
    }

    /**
     * Index the processors discovered via the Java Service Loader by processor name.
     *
     * @return a map of processor name to processor, empty when nothing is discovered
     */
    private static Map<String, ExchangeTemplateProcessor> discoverProcessorsByName() {
        Map<String, ExchangeTemplateProcessor> discoveredProcessors = new HashMap<>();
        try {
            for (ExchangeTemplateProcessor processor : ServiceLoader.load(ExchangeTemplateProcessor.class)) {
                discoveredProcessors.putIfAbsent(processor.getName(), processor);
            }
        } catch (Exception | ServiceConfigurationError e) {
            LOGGER.warn("Failed to discover template processors via ServiceLoader: {}", e.getMessage());
        }
        return discoveredProcessors;
    }

    private static Optional<ExchangeTemplateProcessor> tryCreateBuiltinProcessor(String processorName) {
        try {
            return Optional.of(new ExchangeTemplateProcessorFactory().createBuiltinProcessor(processorName));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
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
            if (manager.getProcessors().stream().noneMatch(p -> p.getName().equals(processor.getName()))) {
                manager.registerProcessor(processor);
            }
        }
        
        return manager;
    }
}