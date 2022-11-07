package com.github.clescot.kafka.connect.http.sink.config;

import org.apache.kafka.common.config.ConfigDef;

public class ConfigDefinition {

    private ConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstants.TARGET_BOOTSTRAP_SERVER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, ConfigConstants.TARGET_BOOTSTRAP_SERVER_DOC)
                .define(ConfigConstants.TARGET_SCHEMA_REGISTRY, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, ConfigConstants.TARGET_SCHEMA_REGISTRY_DOC)
                .define(ConfigConstants.ACK_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, ConfigConstants.ACK_TOPIC_DOC)
                .define(ConfigConstants.ACK_SCHEMA, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, ConfigConstants.ACK_SCHEMA_DOC)
                .define(ConfigConstants.PRODUCER_CLIENT_ID, ConfigDef.Type.STRING, "sinkProducer", ConfigDef.Importance.LOW, ConfigConstants.PRODUCER_CLIENT_ID_DOC);
    }
}
