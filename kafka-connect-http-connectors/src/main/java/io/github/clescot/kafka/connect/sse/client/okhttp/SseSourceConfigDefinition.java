package io.github.clescot.kafka.connect.sse.client.okhttp;

import org.apache.kafka.common.config.ConfigDef;

public class SseSourceConfigDefinition {
    public static final String URL = "url";
    public static final String URL_DOC = "URL of the SSE server to connect to";
    public static final String TOPIC = "topic";
    public static final String TOPIC_DOC = "topic to publish events to";

    public static ConfigDef config() {
        return new ConfigDef()
                .define(URL, ConfigDef.Type.STRING,ConfigDef.Importance.HIGH, URL_DOC)
                .define(TOPIC, ConfigDef.Type.STRING,ConfigDef.Importance.HIGH, TOPIC_DOC)
                ;
    }
}
