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
import static com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class HttpSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkConnectorConfig.class);
    private String queueName;
    private boolean publishToInMemoryQueue;
    private Integer defaultRetries;
    private Long defaultRetryDelayInMs;
    private Long defaultRetryMaxDelayInMs;
    private Double defaultRetryDelayFactor;
    private Long defaultRetryJitterInMs;
    private Long defaultRateLimiterMaxExecutions;
    private Long defaultRateLimiterPeriodInMs;
    private Map<String,List<String>> staticRequestHeaders = Maps.newHashMap();
    private boolean generateMissingRequestId;
    private boolean generateMissingCorrelationId;

    private long maxWaitTimeRegistrationOfQueueConsumerInMs;

    public HttpSinkConnectorConfig(Map<?, ?> originals) {
        this(HttpSinkConfigDefinition.config(), originals);
    }

    public HttpSinkConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.queueName = Optional.ofNullable(getString(QUEUE_NAME)).orElse(DEFAULT_QUEUE_NAME);
        if(queueMapIsEmpty()){
            LOGGER.warn("no pre-existing queue exists. this HttpSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.",queueName);
        }
        this.publishToInMemoryQueue = Optional.ofNullable(getBoolean(PUBLISH_TO_IN_MEMORY_QUEUE)).orElse(false);

        this.defaultRetries = getInt(DEFAULT_RETRIES);
        this.defaultRetryDelayInMs = getLong(DEFAULT_RETRY_DELAY_IN_MS);
        this.defaultRetryMaxDelayInMs = getLong(DEFAULT_RETRY_MAX_DELAY_IN_MS);
        this.defaultRetryDelayFactor = getDouble(DEFAULT_RETRY_DELAY_FACTOR);
        this.defaultRetryJitterInMs = getLong(DEFAULT_RETRY_JITTER_IN_MS);
        this.generateMissingRequestId = getBoolean(GENERATE_MISSING_REQUEST_ID);
        this.generateMissingCorrelationId = getBoolean(GENERATE_MISSING_CORRELATION_ID);
        this.defaultRateLimiterPeriodInMs = getLong(DEFAULT_RATE_LIMITER_PERIOD_IN_MS);
        this.defaultRateLimiterMaxExecutions = getLong(DEFAULT_RATE_LIMITER_MAX_EXECUTIONS);
        this.maxWaitTimeRegistrationOfQueueConsumerInMs = getLong(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS);
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

    public Long getDefaultRateLimiterMaxExecutions() {
        return defaultRateLimiterMaxExecutions;
    }

    public Long getDefaultRateLimiterPeriodInMs() {
        return defaultRateLimiterPeriodInMs;
    }

    public long getMaxWaitTimeRegistrationOfQueueConsumerInMs() {
        return maxWaitTimeRegistrationOfQueueConsumerInMs;
    }
}
