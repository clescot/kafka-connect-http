package io.github.clescot.kafka.connect.sse.client.okhttp;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Optional;

public class SseConnectorConfig extends AbstractConfig {
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String topic;
    private final String url;
    public SseConnectorConfig(Map<?, ?> originals) {
        this(SseConfigDefinition.config(), originals);
    }

    public SseConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.topic = Optional.ofNullable(getString(SseConfigDefinition.TOPIC)).orElseThrow(()-> new IllegalArgumentException(SseConfigDefinition.TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.url  = Optional.ofNullable(getString(SseConfigDefinition.URL)).orElseThrow(()-> new IllegalArgumentException(SseConfigDefinition.URL + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
    }


    public String getTopic() {
        return topic;
    }

    public String getUrl() {
        return url;
    }
}
