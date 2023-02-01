package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import com.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.client.ahc.AHCHttpClientFactory;
import com.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClientFactory;
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

import static com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class HttpSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkConnectorConfig.class);
    private static final String OKHTTP_IMPLEMENTATION = "okhttp";
    private static final String AHC_IMPLEMENTATION = "ahc";
    private final String defaultSuccessResponseCodeRegex;
    private final String defaultRetryResponseCodeRegex;
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
    private int pollDelayRegistrationOfQueueConsumerInMs;
    private int pollIntervalRegistrationOfQueueConsumerInMs;
    private String httpClientFactoryClass;

    public HttpSinkConnectorConfig(Map<?, ?> originals) {
        this(HttpSinkConfigDefinition.config(), originals);
    }

    public HttpSinkConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.queueName = Optional.ofNullable(getString(ConfigConstants.QUEUE_NAME)).orElse(QueueFactory.DEFAULT_QUEUE_NAME);
        if(QueueFactory.queueMapIsEmpty()){
            LOGGER.warn("no pre-existing queue exists. this HttpSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.",queueName);
        }
        this.publishToInMemoryQueue = Optional.ofNullable(getBoolean(PUBLISH_TO_IN_MEMORY_QUEUE)).orElse(false);

        this.defaultRetries = getInt(HTTP_CLIENT_DEFAULT_RETRIES);
        this.defaultRetryDelayInMs = getLong(HTTP_CLIENT_DEFAULT_RETRY_DELAY_IN_MS);
        this.defaultRetryMaxDelayInMs = getLong(HTTP_CLIENT_DEFAULT_RETRY_MAX_DELAY_IN_MS);
        this.defaultRetryDelayFactor = getDouble(HTTP_CLIENT_DEFAULT_RETRY_DELAY_FACTOR);
        this.defaultRetryJitterInMs = getLong(HTTP_CLIENT_DEFAULT_RETRY_JITTER_IN_MS);
        this.generateMissingRequestId = getBoolean(HTTP_CLIENT_GENERATE_MISSING_REQUEST_ID);
        this.generateMissingCorrelationId = getBoolean(HTTP_CLIENT_GENERATE_MISSING_CORRELATION_ID);
        this.defaultRateLimiterPeriodInMs = getLong(HTTP_CLIENT_DEFAULT_RATE_LIMITER_PERIOD_IN_MS);
        this.defaultRateLimiterMaxExecutions = getLong(HTTP_CLIENT_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS);
        this.maxWaitTimeRegistrationOfQueueConsumerInMs = getLong(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        this.pollDelayRegistrationOfQueueConsumerInMs = getInt(POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        this.pollIntervalRegistrationOfQueueConsumerInMs = getInt(POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        Optional<List<String>> staticRequestHeaderNames = Optional.ofNullable(getList(HTTP_CLIENT_STATIC_REQUEST_HEADER_NAMES));
        List<String> additionalHeaderNamesList =staticRequestHeaderNames.orElse(Lists.newArrayList());
        for(String headerName:additionalHeaderNamesList){
            String value = (String) originals().get(headerName);
            Preconditions.checkNotNull(value,"'"+headerName+"' is not configured as a parameter.");
            staticRequestHeaders.put(headerName, Lists.newArrayList(value));
        }
        this.defaultSuccessResponseCodeRegex = getString(HTTP_CLIENT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);
        this.defaultRetryResponseCodeRegex = getString(HTTP_CLIENT_DEFAULT_RETRY_RESPONSE_CODE_REGEX);
        String httpClientImplementation = Optional.ofNullable(getString(HTTPCLIENT_IMPLEMENTATION)).orElse(OKHTTP_IMPLEMENTATION);
        if(AHC_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)){
            this.httpClientFactoryClass = AHCHttpClientFactory.class.getName();
        }else if(OKHTTP_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)){
            this.httpClientFactoryClass = OkHttpClientFactory.class.getName();
        }else{
            LOGGER.error("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '{}'",httpClientImplementation);
            throw new IllegalArgumentException("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '"+httpClientImplementation+"'");
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

    public String getDefaultSuccessResponseCodeRegex() {
        return defaultSuccessResponseCodeRegex;
    }

    public String getDefaultRetryResponseCodeRegex() {
        return defaultRetryResponseCodeRegex;
    }


    public int getPollDelayRegistrationOfQueueConsumerInMs() {
        return pollDelayRegistrationOfQueueConsumerInMs;
    }

    public int getPollIntervalRegistrationOfQueueConsumerInMs() {
        return pollIntervalRegistrationOfQueueConsumerInMs;
    }

    public String getHttpClientFactoryClass() {
        return httpClientFactoryClass;
    }

    @Override
    public String toString() {
        return "HttpSinkConnectorConfig{" +
                "defaultSuccessResponseCodeRegex='" + defaultSuccessResponseCodeRegex + '\'' +
                ", defaultRetryResponseCodeRegex='" + defaultRetryResponseCodeRegex + '\'' +
                ", queueName='" + queueName + '\'' +
                ", publishToInMemoryQueue=" + publishToInMemoryQueue +
                ", defaultRetries=" + defaultRetries +
                ", defaultRetryDelayInMs=" + defaultRetryDelayInMs +
                ", defaultRetryMaxDelayInMs=" + defaultRetryMaxDelayInMs +
                ", defaultRetryDelayFactor=" + defaultRetryDelayFactor +
                ", defaultRetryJitterInMs=" + defaultRetryJitterInMs +
                ", defaultRateLimiterMaxExecutions=" + defaultRateLimiterMaxExecutions +
                ", defaultRateLimiterPeriodInMs=" + defaultRateLimiterPeriodInMs +
                ", staticRequestHeaders=" + staticRequestHeaders +
                ", generateMissingRequestId=" + generateMissingRequestId +
                ", generateMissingCorrelationId=" + generateMissingCorrelationId +
                ", maxWaitTimeRegistrationOfQueueConsumerInMs=" + maxWaitTimeRegistrationOfQueueConsumerInMs +
                '}';
    }


}
