package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.source.cron.HttpCronSourceConfigDefinition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Optional;

public class SseSourceConnectorConfig extends AbstractConfig {
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private String topic;
    private String url;
    public SseSourceConnectorConfig(Map<?, ?> originals) {
        this(SseSourceConfigDefinition.config(), originals);
    }

    public SseSourceConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.topic = Optional.ofNullable(getString(SseSourceConfigDefinition.TOPIC)).orElseThrow(()-> new IllegalArgumentException(SseSourceConfigDefinition.TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.url  = Optional.ofNullable(getString(SseSourceConfigDefinition.URL)).orElseThrow(()-> new IllegalArgumentException(SseSourceConfigDefinition.URL + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
    }


    public String getTopic() {
        return topic;
    }

    public String getUrl() {
        return url;
    }
}
