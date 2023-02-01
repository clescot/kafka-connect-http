package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import org.apache.kafka.common.config.ConfigDef;


public class HttpSourceConfigDefinition {

    public static final String SUCCESS_TOPIC = "success.topic";
    public static final String SUCCESS_TOPIC_DOC = "Topic to receive successful http request/responses";
    public static final String ERROR_TOPIC = "error.topic";
    public static final String ERROR_TOPIC_DOC = "Topic to receive errors from http request/responses";
    private HttpSourceConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(SUCCESS_TOPIC, ConfigDef.Type.STRING,  ConfigDef.Importance.HIGH,SUCCESS_TOPIC_DOC)
                .define(ERROR_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ERROR_TOPIC_DOC)
                .define(ConfigConstants.QUEUE_NAME, ConfigDef.Type.STRING, null,ConfigDef.Importance.MEDIUM, ConfigConstants.QUEUE_NAME_DOC)
                ;
    }
}
