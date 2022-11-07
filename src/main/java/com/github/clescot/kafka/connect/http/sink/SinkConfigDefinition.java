package com.github.clescot.kafka.connect.http.sink;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;

public class SinkConfigDefinition {


    private SinkConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstants.QUEUE_NAME, ConfigDef.Type.STRING, null,ConfigDef.Importance.MEDIUM, ConfigConstants.QUEUE_NAME_DOC)
                .define(ConfigConstants.STATIC_REQUEST_HEADER_NAMES, ConfigDef.Type.LIST,  Collections.emptyList(), ConfigDef.Importance.MEDIUM, ConfigConstants.STATIC_REQUEST_HEADER_NAMES_DOC)
                ;
    }
}
