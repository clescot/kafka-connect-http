package com.github.clescot.kafka.connect.http.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static com.github.clescot.kafka.connect.http.QueueFactory.DEFAULT_QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.QueueFactory.queueMapIsEmpty;
import static com.github.clescot.kafka.connect.http.sink.ConfigConstants.QUEUE_NAME;

public class WsSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(WsSinkConnectorConfig.class);
    private String queueName;


    public WsSinkConnectorConfig(Map<?, ?> originals) {
        this(SinkConfigDefinition.config(), originals);
    }

    public WsSinkConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.queueName = Optional.ofNullable(getString(QUEUE_NAME)).orElse(DEFAULT_QUEUE_NAME);
        if(queueMapIsEmpty()){
            LOGGER.warn("no pre-existing queue exists. this WsSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.",queueName);
        }
    }





    public String getQueueName() {
        return queueName;
    }
}
