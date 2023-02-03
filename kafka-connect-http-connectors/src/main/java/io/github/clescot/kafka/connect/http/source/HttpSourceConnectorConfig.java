package io.github.clescot.kafka.connect.http.source;

import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class HttpSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSourceConnectorConfig.class);
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String successTopic;
    private final String errorsTopic;
    private final String queueName;


    public HttpSourceConnectorConfig(Map<?, ?> originals) {
        this(HttpSourceConfigDefinition.config(), originals);
    }

    public HttpSourceConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.successTopic = Optional.ofNullable(getString(HttpSourceConfigDefinition.SUCCESS_TOPIC)).orElseThrow(()-> new IllegalArgumentException(HttpSourceConfigDefinition.SUCCESS_TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.errorsTopic = Optional.ofNullable(getString(HttpSourceConfigDefinition.ERROR_TOPIC)).orElseThrow(()-> new IllegalArgumentException(HttpSourceConfigDefinition.ERROR_TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.queueName = Optional.ofNullable(getString(ConfigConstants.QUEUE_NAME)).orElse(QueueFactory.DEFAULT_QUEUE_NAME);
        if(QueueFactory.queueMapIsEmpty()){
            LOGGER.warn("no pre-existing queue exists. this HttpSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.",queueName);
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
