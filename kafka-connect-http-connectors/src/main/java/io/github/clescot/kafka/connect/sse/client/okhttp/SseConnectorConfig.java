package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.collect.Lists;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.CONFIGURATION_IDS;

public class SseConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(SseConnectorConfig.class);
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final List<String> configurationIds;
    private final String topic;
    private final String url;
    public SseConnectorConfig(Map<String, String> originals) {
        this(new SseConfigDefinition(originals).config(), originals);
    }

    public SseConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef, originals, LOGGER.isDebugEnabled());
        this.configurationIds = Optional.ofNullable(getList(CONFIGURATION_IDS)).orElse(Lists.newArrayList());
        this.topic = Optional.ofNullable(getString(SseConfigDefinition.DEFAULT_CONFIG_TOPIC)).orElseThrow(()-> new IllegalArgumentException(SseConfigDefinition.TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.url  = Optional.ofNullable(getString(SseConfigDefinition.DEFAULT_CONFIG_URL)).orElseThrow(()-> new IllegalArgumentException(SseConfigDefinition.URL + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
    }


    public String getDefaultTopic() {
        return topic;
    }

    public String getDefaultUrl() {
        return url;
    }
}
