package com.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.clescot.kafka.connect.http.ConfigConstants.QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.QueueFactory.DEFAULT_QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.QueueFactory.queueMapIsEmpty;
import static com.github.clescot.kafka.connect.http.sink.WsSinkConfigDefinition.*;

public class WsSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(WsSinkConnectorConfig.class);
    private final String queueName;
    private final boolean publishToInMemoryQueue;
    private final Integer defaultRetries;
    private final Long defaultRetryDelayInMs;
    private final Long defaultRetryMaxDelayInMs;
    private final Double defaultRetryDelayFactor;
    private final Long defaultRetryJitterInMs;
    private final Map<String,List<String>> staticRequestHeaders = Maps.newHashMap();
    private final boolean generateMissingRequestId;
    private final boolean generateMissingCorrelationId;

    public WsSinkConnectorConfig(Map<?, ?> originals) {
        this(WsSinkConfigDefinition.config(), originals);
    }

    public WsSinkConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.queueName = Optional.ofNullable(getString(QUEUE_NAME)).orElse(DEFAULT_QUEUE_NAME);
        if(queueMapIsEmpty()){
            LOGGER.warn("no pre-existing queue exists. this WsSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.",queueName);
        }
        this.publishToInMemoryQueue = Optional.ofNullable(getBoolean(PUBLISH_TO_IN_MEMORY_QUEUE)).orElse(false);

        this.defaultRetries = Optional.ofNullable(getInt(DEFAULT_RETRIES)).orElse(null);
        this.defaultRetryDelayInMs = Optional.ofNullable(getLong(DEFAULT_RETRY_DELAY_IN_MS)).orElse(null);
        this.defaultRetryMaxDelayInMs = Optional.ofNullable(getLong(DEFAULT_RETRY_MAX_DELAY_IN_MS)).orElse(null);
        this.defaultRetryDelayFactor = Optional.ofNullable(getDouble(DEFAULT_RETRY_DELAY_FACTOR)).orElse(null);
        this.defaultRetryJitterInMs = Optional.ofNullable(getLong(DEFAULT_RETRY_JITTER_IN_MS)).orElse(null);
        this.generateMissingRequestId = getBoolean(GENERATE_MISSING_REQUEST_ID);
        this.generateMissingCorrelationId = getBoolean(GENERATE_MISSING_CORRELATION_ID);
        //TODO add Throttling parameters
        Optional<List<String>> staticRequestHeaderNames = Optional.ofNullable(getList(STATIC_REQUEST_HEADER_NAMES));
        List<String> additionalHeaderNamesList =staticRequestHeaderNames.orElse(Lists.newArrayList());
        for(String headerName:additionalHeaderNamesList){
            String value = (String) originals().get(headerName);
            Preconditions.checkNotNull(value,"'"+headerName+"' is not configured as a parameter.");
            staticRequestHeaders.put(headerName, Lists.newArrayList(value));
        }
    }

    public String getQueueName() {
        return queueName;
    }

    public boolean isPublishToInMemoryQueue() {
        return publishToInMemoryQueue;
    }

    public Map<String, List<String>> getStaticRequestHeaders() {
        return Maps.newHashMap(staticRequestHeaders);
    }

    public Integer getDefaultRetries() {
        return defaultRetries;
    }

    public Long getDefaultRetryDelayInMs() {
        return defaultRetryDelayInMs;
    }

    public Long getDefaultRetryMaxDelayInMs() {
        return defaultRetryMaxDelayInMs;
    }

    public Double getDefaultRetryDelayFactor() {
        return defaultRetryDelayFactor;
    }

    public Long getDefaultRetryJitterInMs() {
        return defaultRetryJitterInMs;
    }

    public boolean isGenerateMissingRequestId() {
        return generateMissingRequestId;
    }

    public boolean isGenerateMissingCorrelationId() {
        return generateMissingCorrelationId;
    }
}
