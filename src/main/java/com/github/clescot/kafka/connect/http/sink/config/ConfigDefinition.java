package com.github.clescot.kafka.connect.http.sink.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.storage.StringConverter;

public class ConfigDefinition {

    public static final String DEFAULT_PRODUCER_CLIENT_ID = "httpSinkProducer";

    private ConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstants.ACK_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, ConfigConstants.ACK_TOPIC_DOC);
    }
}
