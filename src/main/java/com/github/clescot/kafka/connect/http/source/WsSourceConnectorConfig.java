package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.sink.ConfigConstants;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static com.github.clescot.kafka.connect.http.QueueFactory.DEFAULT_QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.QueueFactory.queueMapIsEmpty;
import static com.github.clescot.kafka.connect.http.sink.ConfigConstants.QUEUE_NAME;

public class WsSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(WsSourceConnectorConfig.class);
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private String successTopic;
    private String errorsTopic;
    private String queueName;


    public WsSourceConnectorConfig(Map<?, ?> originals) {
        this(SourceConfigDefinition.config(), originals);
    }

    public WsSourceConnectorConfig(ConfigDef configDef,Map<?, ?> originals){
        super(configDef,originals);
        this.successTopic = Optional.ofNullable(getString(ConfigConstants.SUCCESS_TOPIC)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.SUCCESS_TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.errorsTopic = Optional.ofNullable(getString(ConfigConstants.ERRORS_TOPIC)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.ERRORS_TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.queueName = Optional.ofNullable(getString(QUEUE_NAME)).orElse(DEFAULT_QUEUE_NAME);
        if(queueMapIsEmpty()){
            LOGGER.warn("no pre-existing queue exists. this WsSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.",queueName);
        }
    }




    public String getSuccessTopic() {
        return successTopic;
    }

    public String getErrorsTopic() {
        return errorsTopic;
    }

    public String getQueueName() {
        return queueName;
    }
}
