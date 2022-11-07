package com.github.clescot.kafka.connect.http.sink.config;

import org.apache.kafka.common.config.ConfigDef;

public class ConfigDefinition {


    private ConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstants.SUCCESS_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, ConfigConstants.SUCCESS_TOPIC_DOC)
                .define(ConfigConstants.ERRORS_TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, ConfigConstants.ERRORS_TOPIC_DOC)
                ;
    }
}
