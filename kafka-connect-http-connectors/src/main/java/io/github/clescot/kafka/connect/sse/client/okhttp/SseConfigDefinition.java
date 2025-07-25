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

    public static final String DEFAULT_CONFIG_TOPIC = DEFAULT_CONFIGURATION_PREFIX+"topic";
    public static final String TOPIC_DOC = "topic to publish events to";

    //error strategy
    public static final String ERROR_STRATEGY = "error.strategy";
    public static final String ERROR_STRATEGY_ALWAYS_CONTINUE = "always-continue";
    public static final String ERROR_STRATEGY_ALWAYS_THROW = "always-throw";
    public static final String ERROR_STRATEGY_CONTINUE_WITH_MAX_ATTEMPTS = "continue-with-max-attempts";
    public static final String ERROR_STRATEGY_MAX_ATTEMPTS = "max-attempts";
    public static final String ERROR_STRATEGY_TIME_LIMIT_COUNT_IN_MILLIS = "time-limit-count-in-millis";
    public static final String ERROR_STRATEGY_CONTINUE_WITH_TIME_LIMIT = "continue-with-time-limit";

    //retry delay strategy
    public static final String RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS = "retry.delay.strategy.max-delay-millis";
    public static final String RETRY_DELAY_STRATEGY_BACKOFF_MULTIPLIER = "retry.delay.strategy.backoff-multiplier";
    public static final String RETRY_DELAY_STRATEGY_JITTER_MULTIPLIER = "retry.delay.strategy.jitter-multiplier";


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
                .define(prefix + ERROR_STRATEGY, ConfigDef.Type.STRING,ERROR_STRATEGY_ALWAYS_CONTINUE, ConfigDef.Importance.MEDIUM, TOPIC_DOC)
                ;
    }
}
