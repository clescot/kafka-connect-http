package com.github.clescot.kafka.connect.http.sink;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;

public class WsSinkConfigDefinition {


    private WsSinkConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstants.QUEUE_NAME, ConfigDef.Type.STRING, null,ConfigDef.Importance.MEDIUM, ConfigConstants.QUEUE_NAME_DOC)
                .define(ConfigConstants.STATIC_REQUEST_HEADER_NAMES, ConfigDef.Type.LIST,  Collections.emptyList(), ConfigDef.Importance.MEDIUM, ConfigConstants.STATIC_REQUEST_HEADER_NAMES_DOC)
                .define(ConfigConstants.PUBLISH_TO_IN_MEMORY_QUEUE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, ConfigConstants.PUBLISH_TO_IN_MEMORY_QUEUE_DOC)
                ;
    }
}
