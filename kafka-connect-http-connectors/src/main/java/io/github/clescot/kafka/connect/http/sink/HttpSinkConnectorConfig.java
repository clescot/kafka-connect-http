package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class HttpSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkConnectorConfig.class);


    private final String defaultSuccessResponseCodeRegex;
    private final String defaultRetryResponseCodeRegex;
    private final String queueName;
    private final boolean publishToInMemoryQueue;
    private final Integer defaultRetries;
    private final Long defaultRetryDelayInMs;
    private final Long defaultRetryMaxDelayInMs;
    private final Double defaultRetryDelayFactor;
    private final Long defaultRetryJitterInMs;
    private final Long defaultRateLimiterMaxExecutions;
    private final String defaultRateLimiterScope;
    private final Long defaultRateLimiterPeriodInMs;
    private final Map<String, List<String>> staticRequestHeaders = Maps.newHashMap();
    private final boolean generateMissingRequestId;
    private final boolean generateMissingCorrelationId;

    private final long maxWaitTimeRegistrationOfQueueConsumerInMs;
    private final int pollDelayRegistrationOfQueueConsumerInMs;
    private final int pollIntervalRegistrationOfQueueConsumerInMs;
    private final Integer customFixedThreadpoolSize;
    private final List<String> configurationIds;

    public HttpSinkConnectorConfig(Map<?, ?> originals) {
        this(HttpSinkConfigDefinition.config(), originals);
    }

    public HttpSinkConnectorConfig(ConfigDef configDef, Map<?, ?> originals) {
        super(configDef, originals, LOGGER.isDebugEnabled());
        this.queueName = Optional.ofNullable(getString(ConfigConstants.QUEUE_NAME)).orElse(QueueFactory.DEFAULT_QUEUE_NAME);
        if (QueueFactory.queueMapIsEmpty()) {
            LOGGER.warn("no pre-existing queue exists. this HttpSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.", queueName);
        }
        this.publishToInMemoryQueue = Optional.ofNullable(getBoolean(PUBLISH_TO_IN_MEMORY_QUEUE)).orElse(false);

        this.defaultRetries = getInt(CONFIG_DEFAULT_RETRIES);
        this.defaultRetryDelayInMs = getLong(CONFIG_DEFAULT_RETRY_DELAY_IN_MS);
        this.defaultRetryMaxDelayInMs = getLong(CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS);
        this.defaultRetryDelayFactor = getDouble(CONFIG_DEFAULT_RETRY_DELAY_FACTOR);
        this.defaultRetryJitterInMs = getLong(CONFIG_DEFAULT_RETRY_JITTER_IN_MS);
        this.generateMissingRequestId = getBoolean(CONFIG_GENERATE_MISSING_REQUEST_ID);
        this.generateMissingCorrelationId = getBoolean(CONFIG_GENERATE_MISSING_CORRELATION_ID);
        this.defaultRateLimiterPeriodInMs = getLong(CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS);
        this.defaultRateLimiterMaxExecutions = getLong(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS);
        this.defaultRateLimiterScope = getString(CONFIG_DEFAULT_RATE_LIMITER_SCOPE);
        this.maxWaitTimeRegistrationOfQueueConsumerInMs = getLong(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        this.pollDelayRegistrationOfQueueConsumerInMs = getInt(POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        this.pollIntervalRegistrationOfQueueConsumerInMs = getInt(POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        Optional<List<String>> staticRequestHeaderNames = Optional.ofNullable(getList(CONFIG_STATIC_REQUEST_HEADER_NAMES));
        List<String> additionalHeaderNamesList = staticRequestHeaderNames.orElse(Lists.newArrayList());
        for (String headerName : additionalHeaderNamesList) {
            String value = (String) originals().get(DEFAULT_CONFIGURATION_PREFIX+STATIC_REQUEST_HEADER_PREFIX+headerName);
            Preconditions.checkNotNull(value, "'" + headerName + "' is not configured as a parameter.");
            staticRequestHeaders.put(headerName, Lists.newArrayList(value));
        }
        this.defaultSuccessResponseCodeRegex = getString(CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);
        this.defaultRetryResponseCodeRegex = getString(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX);

        this.customFixedThreadpoolSize = getInt(CONFIG_HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE);
        configurationIds = Optional.ofNullable(getList(CONFIGURATION_IDS)).orElse(Lists.newArrayList());

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


    public Integer getCustomFixedThreadpoolSize() {
        return customFixedThreadpoolSize;
    }

    public List<String> getConfigurationIds() {
        return configurationIds;
    }

    public String getDefaultRateLimiterScope() {
        return defaultRateLimiterScope;
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
                ", defaultRateLimiterScope=" + defaultRateLimiterScope +
                ", staticRequestHeaders=" + staticRequestHeaders +
                ", generateMissingRequestId=" + generateMissingRequestId +
                ", generateMissingCorrelationId=" + generateMissingCorrelationId +
                ", maxWaitTimeRegistrationOfQueueConsumerInMs=" + maxWaitTimeRegistrationOfQueueConsumerInMs +
                ", customFixedThreadpoolSize=" + customFixedThreadpoolSize +
                ", configurationIds=" + configurationIds +
                '}';
    }


}
