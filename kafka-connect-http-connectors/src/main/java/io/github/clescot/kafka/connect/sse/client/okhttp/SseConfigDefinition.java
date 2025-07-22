package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.sink.HttpClientConfigDefinition;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class SseConfigDefinition {
    public static final String URL = "url";
    public static final String URL_DOC = "URL of the SSE server to connect to";
    public static final String TOPIC = "topic";
    public static final String TOPIC_DOC = "topic to publish events to";
    private final Map<String, String> settings;

    public SseConfigDefinition(Map<String, String> settings) {
        this.settings = settings;
    }

    public ConfigDef config() {
        HttpClientConfigDefinition httpClientConfigDefinition = new HttpClientConfigDefinition(settings);
        return httpClientConfigDefinition.config()
                .define(URL, ConfigDef.Type.STRING,ConfigDef.Importance.HIGH, URL_DOC)
                .define(TOPIC, ConfigDef.Type.STRING,ConfigDef.Importance.HIGH, TOPIC_DOC);
    }
}
