package com.github.clescot.kafka.connect.http.sink.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;

public class ConfigDefinition {


    private ConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstants.SUCCESS_TOPIC, ConfigDef.Type.STRING, null, new ConfigDef.NonNullValidator(),ConfigDef.Importance.HIGH, ConfigConstants.SUCCESS_TOPIC_DOC)
                .define(ConfigConstants.ERRORS_TOPIC, ConfigDef.Type.STRING, null, new ConfigDef.NonNullValidator(),ConfigDef.Importance.HIGH, ConfigConstants.ERRORS_TOPIC_DOC)
                .define(ConfigConstants.QUEUE_NAME, ConfigDef.Type.STRING, null,ConfigDef.Importance.MEDIUM, ConfigConstants.QUEUE_NAME_DOC)
                .define(ConfigConstants.STATIC_REQUEST_HEADERS, ConfigDef.Type.LIST,  Collections.emptyList(), ConfigDef.Importance.MEDIUM, ConfigConstants.STATIC_REQUEST_HEADERS_DOC)
                ;
    }
}
