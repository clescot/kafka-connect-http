package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Sets;
import io.github.clescot.kafka.connect.ConfigUtils;
import io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition;
import io.github.clescot.kafka.connect.http.sink.SinkConfigDefinition;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.CONFIGURATION_IDS;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.DEFAULT_CONFIGURATION_PREFIX;

public class SseConfigDefinition {
    public static final String URL ="url";
    public static final String DEFAULT_CONFIG_URL = DEFAULT_CONFIGURATION_PREFIX+URL;
    public static final String URL_DOC = "URL of the SSE server to connect to";
    public static final String TOPIC = "topic";

    public static final String DEFAULT_CONFIG_TOPIC = DEFAULT_CONFIGURATION_PREFIX+TOPIC;
    public static final String TOPIC_DOC = "topic to publish events to";

    //error strategy
    public static final String ERROR_STRATEGY = "error.strategy";
    public static final String ERROR_STRATEGY_DOC = "Strategy to handle errors during SSE event processing. ";
    public static final String ERROR_STRATEGY_ALWAYS_CONTINUE = "always-continue";
    public static final String ERROR_STRATEGY_ALWAYS_CONTINUE_DOC = "Always continue processing events, even if an error occurs. " +
            "This strategy will log the error and continue to the next event without stopping the processing.";
    public static final String ERROR_STRATEGY_ALWAYS_THROW = "always-throw";
    public static final String ERROR_STRATEGY_ALWAYS_THROW_DOC = "Always throw an exception when an error occurs during SSE event processing. " +
            "This strategy will stop the processing and throw an exception, which can be caught by the caller.";
    public static final String ERROR_STRATEGY_CONTINUE_WITH_MAX_ATTEMPTS = "continue-with-max-attempts";
    public static final String ERROR_STRATEGY_CONTINUE_WITH_MAX_ATTEMPTS_DOC = "Continue processing events, but limit the number of attempts to process an event before giving up. " +
            "This strategy will log the error and continue to the next event after the maximum number of attempts is reached.";
    public static final String ERROR_STRATEGY_MAX_ATTEMPTS = "error.strategy.max-attempts";
    public static final String ERROR_STRATEGY_MAX_ATTEMPTS_DOC = "Maximum number of attempts to process an event before giving up. " +
            "This setting is only used when the error strategy is set to 'continue-with-max-attempts'.";
    public static final String ERROR_STRATEGY_TIME_LIMIT_COUNT_IN_MILLIS = "error.strategy.time-limit-count-in-millis";
    public static final String ERROR_STRATEGY_TIME_LIMIT_COUNT_IN_MILLIS_DOC = "Time limit in milliseconds for processing an event before giving up. " +
            "This setting is only used when the error strategy is set to 'continue-with-time-limit'.";
    public static final String ERROR_STRATEGY_CONTINUE_WITH_TIME_LIMIT = "continue-with-time-limit";
    public static final String ERROR_STRATEGY_CONTINUE_WITH_TIME_LIMIT_DOC = "Continue processing events, but limit the time spent on processing an event before giving up. " +
            "This strategy will log the error and continue to the next event after the time limit is reached.";

    //retry delay strategy
    public static final String RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS = "retry.delay.strategy.max-delay-millis";
    public static final String RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS_DOC = "Maximum delay in milliseconds between retries when an error occurs during SSE event processing. " +
            "This setting is used to control the maximum delay before retrying to process an event after an error.";
    public static final String RETRY_DELAY_STRATEGY_BACKOFF_MULTIPLIER = "retry.delay.strategy.backoff-multiplier";
    public static final String RETRY_DELAY_STRATEGY_BACKOFF_MULTIPLIER_DOC = "Multiplier for the backoff delay when retrying to process an event after an error. " +
            "This setting is used to increase the delay between retries exponentially.";
    public static final String RETRY_DELAY_STRATEGY_JITTER_MULTIPLIER = "retry.delay.strategy.jitter-multiplier";
    public static final String RETRY_DELAY_STRATEGY_JITTER_MULTIPLIER_DOC = "Multiplier for the jitter delay when retrying to process an event after an error. " +
            "This setting is used to add randomness to the delay between retries to avoid thundering herd problems.";


    private final Map<String, String> settings;

    public SseConfigDefinition(Map<String, String> settings) {
        this.settings = settings;
    }

    public ConfigDef config() {
        HttpClientConfigDefinition httpClientConfigDefinition = new HttpClientConfigDefinition(settings);
        SinkConfigDefinition sinkConfigDefinition = new SinkConfigDefinition();


        ConfigDef configDef = new ConfigDef();

        //custom configurations
        String configurationIds = settings.get(CONFIGURATION_IDS);
        Set<String> configs = Sets.newHashSet();
        if (configurationIds != null) {
            configs.addAll(Arrays.asList(configurationIds.split(",")));
        }
        configs.add("default");
        for (String configurationName : configs) {
            configDef = appendConfigurationConfigDef(configDef, configurationName);
        }

        return ConfigUtils.mergeConfigDefs(httpClientConfigDefinition.config(),sinkConfigDefinition.config(),configDef);
    }

    private ConfigDef appendConfigurationConfigDef(ConfigDef configDef, String configurationName) {
        String prefix = "config." + configurationName + ".";
        return configDef
                .define(prefix + URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, URL_DOC)
                .define(prefix + TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)
                //error strategy
                .define(prefix + ERROR_STRATEGY, ConfigDef.Type.STRING,ERROR_STRATEGY_ALWAYS_CONTINUE, ConfigDef.Importance.MEDIUM, ERROR_STRATEGY_DOC)
                .define(prefix + ERROR_STRATEGY_MAX_ATTEMPTS, ConfigDef.Type.INT,3, ConfigDef.Importance.MEDIUM, ERROR_STRATEGY_MAX_ATTEMPTS_DOC)
                .define(prefix + ERROR_STRATEGY_TIME_LIMIT_COUNT_IN_MILLIS, ConfigDef.Type.INT,60000, ConfigDef.Importance.MEDIUM, ERROR_STRATEGY_TIME_LIMIT_COUNT_IN_MILLIS_DOC)
                //retry delay strategy
                .define(prefix + RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS, ConfigDef.Type.LONG,30000L, ConfigDef.Importance.MEDIUM, RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS_DOC)
                .define(prefix + RETRY_DELAY_STRATEGY_BACKOFF_MULTIPLIER, ConfigDef.Type.INT,2, ConfigDef.Importance.MEDIUM, RETRY_DELAY_STRATEGY_BACKOFF_MULTIPLIER_DOC)
                .define(prefix + RETRY_DELAY_STRATEGY_JITTER_MULTIPLIER, ConfigDef.Type.DOUBLE,0.5, ConfigDef.Importance.MEDIUM, RETRY_DELAY_STRATEGY_JITTER_MULTIPLIER_DOC)
                ;
    }
}
