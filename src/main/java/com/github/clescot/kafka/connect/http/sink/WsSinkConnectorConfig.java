package com.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.github.clescot.kafka.connect.http.QueueFactory.DEFAULT_QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.QueueFactory.queueMapIsEmpty;
import static com.github.clescot.kafka.connect.http.sink.ConfigConstants.*;

public class WsSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(WsSinkConnectorConfig.class);
    private String queueName;
    private boolean ignoreHttpResponses;
    private Map<String,String> staticRequestHeaders = Maps.newHashMap();

    public WsSinkConnectorConfig(Map<?, ?> originals) {
        this(WsSinkConfigDefinition.config(), originals);
    }

    public WsSinkConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.queueName = Optional.ofNullable(getString(QUEUE_NAME)).orElse(DEFAULT_QUEUE_NAME);
        if(queueMapIsEmpty()){
            LOGGER.warn("no pre-existing queue exists. this WsSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.",queueName);
        }
        this.ignoreHttpResponses = Optional.ofNullable(getBoolean(IGNORE_HTTP_RESPONSES)).orElse(true);

        Optional<List<String>> staticRequestHeaderNames = Optional.ofNullable(getList(STATIC_REQUEST_HEADER_NAMES));
        List<String> additionalHeaderNamesList =staticRequestHeaderNames.orElse(Lists.newArrayList());
        for(String headerName:additionalHeaderNamesList){
            String value = (String) originals().get(headerName);
            Preconditions.checkNotNull(value,"'"+headerName+"' is not configured as a parameter.");
            staticRequestHeaders.put(headerName, value);
        }
    }

    public String getQueueName() {
        return queueName;
    }

    public boolean isIgnoreHttpResponses() {
        return ignoreHttpResponses;
    }

    public Map<String, String> getStaticRequestHeaders() {
        return Maps.newHashMap(staticRequestHeaders);
    }
}
