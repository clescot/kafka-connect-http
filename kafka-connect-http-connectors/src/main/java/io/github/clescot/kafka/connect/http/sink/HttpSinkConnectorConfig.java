package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.sink.mapper.MapperMode;
import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class HttpSinkConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkConnectorConfig.class);
    private final String producerFormat;
    private final String producerContent;

    //publish mode set to 'producer'
    private final String producerBootstrapServers;
    private final String producerSuccessTopic;
    private final String producerErrorTopic;
    private final String producerSchemaRegistryUrl;
    private final int producerSchemaRegistryCacheCapacity;
    private final boolean producerSchemaRegistryautoRegister;
    private final String producerJsonSchemaSpecVersion;
    private final boolean producerJsonWriteDatesAs8601;
    private final boolean producerJsonOneOfForNullables;
    private final boolean producerJsonFailInvalidSchema;
    private final boolean producerJsonFailUnknownProperties;
    private final String producerKeySubjectNameStrategy;
    private final String producerValueSubjectNameStrategy;
    private final Long missingIdCacheTTLSec;
    private final Long missingVersionCacheTTLSec;
    private final Long missingSchemaCacheTTLSec;
    private final Integer missingCacheSize;
    private final Integer bearerAuthCacheExpiryBufferSeconds;
    private final String bearerAuthScopeClaimName;
    private final String bearerAuthSubClaimName;


    private final boolean meterRegistryExporterJmxActivate;
    private final boolean meterRegistryExporterPrometheusActivate;
    private final int meterRegistryExporterPrometheusPort;
    private final boolean meterRegistryBindMetricsExecutorService;
    private final boolean meterRegistryBindMetricsJvmClassloader;
    private final boolean meterRegistryBindMetricsJvmProcessor;
    private final boolean meterRegistryBindMetricsJvmGc;
    private final boolean meterRegistryBindMetricsJvmInfo;
    private final boolean meterRegistryBindMetricsJvmMemory;
    private final boolean meterRegistryBindMetricsJvmThread;
    private final boolean meterRegistryBindMetricsLogback;
    private final boolean meterRegistryTagIncludeLegacyHost;
    private final boolean meterRegistryTagIncludeUrlPath;

    private final String httpClientImplementation;
    private final String defaultSuccessResponseCodeRegex;
    private final String defaultRetryResponseCodeRegex;
    private final String queueName;
    private final PublishMode publishMode;
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
    private final List<String> httpRequestMapperIds;
    private final List<String> messageSplitterIds;
    private final MapperMode defaultRequestMapperMode;
    private final String defaultUrlExpression;
    private final String defaultMethodExpression;
    private final String defaultBodyTypeExpression;
    private final String defaultBodyExpression;
    private final String defaultHeadersExpression;
    private final Boolean producerJsonIndentOutput;

    public HttpSinkConnectorConfig(Map<String, String> originals) {
        this(new HttpSinkConfigDefinition(originals).config(), originals);
    }

    public HttpSinkConnectorConfig(ConfigDef configDef, Map<String, String> originals) {
        super(configDef, originals, LOGGER.isDebugEnabled());


        //producer
        this.producerBootstrapServers = getString(PRODUCER_BOOTSTRAP_SERVERS);
        this.producerSchemaRegistryUrl = getString(PRODUCER_SCHEMA_REGISTRY_URL);
        this.producerSchemaRegistryCacheCapacity = getInt(PRODUCER_SCHEMA_REGISTRY_CACHE_CAPACITY);
        this.producerSchemaRegistryautoRegister = getBoolean(PRODUCER_SCHEMA_REGISTRY_AUTO_REGISTER);
        this.producerFormat = getString(PRODUCER_FORMAT);
        this.producerContent = getString(PRODUCER_CONTENT);
        this.producerJsonSchemaSpecVersion = getString(PRODUCER_JSON_SCHEMA_SPEC_VERSION);
        this.producerJsonWriteDatesAs8601 = getBoolean(PRODUCER_JSON_WRITE_DATES_AS_ISO_8601);
        this.producerJsonOneOfForNullables = getBoolean(PRODUCER_JSON_ONE_OF_FOR_NULLABLES);
        this.producerJsonFailInvalidSchema = getBoolean(PRODUCER_JSON_FAIL_INVALID_SCHEMA);
        this.producerJsonFailUnknownProperties = getBoolean(PRODUCER_JSON_FAIL_UNKNOWN_PROPERTIES);
        this.producerJsonIndentOutput = getBoolean(PRODUCER_JSON_INDENT_OUTPUT);
        this.producerKeySubjectNameStrategy = getString(PRODUCER_KEY_SUBJECT_NAME_STRATEGY);
        this.producerValueSubjectNameStrategy = getString(PRODUCER_VALUE_SUBJECT_NAME_STRATEGY);
        this.missingIdCacheTTLSec = getLong(PRODUCER_MISSING_ID_CACHE_TTL_SEC);
        this.missingVersionCacheTTLSec = getLong(PRODUCER_MISSING_VERSION_CACHE_TTL_SEC);
        this.missingSchemaCacheTTLSec = getLong(PRODUCER_MISSING_SCHEMA_CACHE_TTL_SEC);
        this.missingCacheSize = getInt(PRODUCER_MISSING_CACHE_SIZE);
        this.bearerAuthCacheExpiryBufferSeconds = getInt(PRODUCER_BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS);
        this.bearerAuthScopeClaimName = getString(PRODUCER_BEARER_AUTH_SCOPE_CLAIM_NAME);
        this.bearerAuthSubClaimName = getString(PRODUCER_BEARER_AUTH_SUB_CLAIM_NAME);

        //meter Registry
        this.meterRegistryExporterJmxActivate = Boolean.parseBoolean(getString(METER_REGISTRY_EXPORTER_JMX_ACTIVATE));
        this.meterRegistryExporterPrometheusActivate = Boolean.parseBoolean(getString(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE));
        this.meterRegistryExporterPrometheusPort = getInt(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT);
        this.meterRegistryBindMetricsExecutorService = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE));
        this.meterRegistryBindMetricsJvmClassloader = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER));
        this.meterRegistryBindMetricsJvmProcessor = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR));
        this.meterRegistryBindMetricsJvmGc = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_JVM_GC));
        this.meterRegistryBindMetricsJvmInfo = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_JVM_INFO));
        this.meterRegistryBindMetricsJvmMemory = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_JVM_MEMORY));
        this.meterRegistryBindMetricsJvmThread = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_JVM_THREAD));
        this.meterRegistryBindMetricsLogback = Boolean.parseBoolean(getString(METER_REGISTRY_BIND_METRICS_LOGBACK));
        this.meterRegistryTagIncludeLegacyHost = Boolean.parseBoolean(getString(METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST));
        this.meterRegistryTagIncludeUrlPath = Boolean.parseBoolean(getString(METER_REGISTRY_TAG_INCLUDE_URL_PATH));

        this.httpClientImplementation = getString(CONFIG_HTTP_CLIENT_IMPLEMENTATION);

        this.queueName = Optional.ofNullable(getString(ConfigConstants.QUEUE_NAME)).orElse(QueueFactory.DEFAULT_QUEUE_NAME);
        this.publishMode = PublishMode.valueOf(Optional.ofNullable(getString(PUBLISH_MODE)).orElse(PublishMode.NONE.name()));
        this.producerSuccessTopic = getString(PRODUCER_SUCCESS_TOPIC);
        this.producerErrorTopic = getString(PRODUCER_ERROR_TOPIC);
        if (QueueFactory.queueMapIsEmpty() && PublishMode.IN_MEMORY_QUEUE.name().equalsIgnoreCase(publishMode.name())) {
            LOGGER.warn("no pre-existing queue exists. this HttpSourceConnector has created a '{}' one. It needs to consume a queue filled with a SinkConnector. Ignore this message if a SinkConnector will be created after this one.", queueName);
        }

        this.defaultRetries = getInt(CONFIG_DEFAULT_RETRIES);
        this.defaultRetryDelayInMs = getLong(CONFIG_DEFAULT_RETRY_DELAY_IN_MS);
        this.defaultRetryMaxDelayInMs = getLong(CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS);
        this.defaultRetryDelayFactor = getDouble(CONFIG_DEFAULT_RETRY_DELAY_FACTOR);
        this.defaultRetryJitterInMs = getLong(CONFIG_DEFAULT_RETRY_JITTER_IN_MS);
        this.generateMissingRequestId = Boolean.parseBoolean(getString(CONFIG_GENERATE_MISSING_REQUEST_ID));
        this.generateMissingCorrelationId = Boolean.parseBoolean(getString(CONFIG_GENERATE_MISSING_CORRELATION_ID));
        this.defaultRateLimiterPeriodInMs = getLong(CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS);
        this.defaultRateLimiterMaxExecutions = getLong(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS);
        this.defaultRateLimiterScope = getString(CONFIG_DEFAULT_RATE_LIMITER_SCOPE);
        this.maxWaitTimeRegistrationOfQueueConsumerInMs = getLong(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        this.pollDelayRegistrationOfQueueConsumerInMs = getInt(POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        this.pollIntervalRegistrationOfQueueConsumerInMs = getInt(POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS);
        Optional<List<String>> staticRequestHeaderNames = Optional.ofNullable(getList(CONFIG_STATIC_REQUEST_HEADER_NAMES));
        List<String> additionalHeaderNamesList = staticRequestHeaderNames.orElse(Lists.newArrayList());
        String originalStrings = Joiner.on(",\n").join(originals().entrySet());
        for (String headerName : additionalHeaderNamesList) {
            String key = DEFAULT_CONFIGURATION_PREFIX + STATIC_REQUEST_HEADER_PREFIX + headerName;
            String value = (String) originals().get(key);
            Preconditions.checkNotNull(value, "'" + key + "' is not configured as a parameter. original parameters : \n" + originalStrings);
            staticRequestHeaders.put(headerName, Lists.newArrayList(value));
        }
        this.defaultSuccessResponseCodeRegex = getString(CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);
        this.defaultRetryResponseCodeRegex = getString(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX);
        this.customFixedThreadpoolSize = getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE);
        this.configurationIds = Optional.ofNullable(getList(CONFIGURATION_IDS)).orElse(Lists.newArrayList());
        this.defaultRequestMapperMode = Optional.of(MapperMode.valueOf(getString(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_MODE))).orElse(MapperMode.DIRECT);
        this.defaultUrlExpression = getString(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION);
        this.defaultMethodExpression = getString(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION);
        this.defaultBodyTypeExpression = Optional.ofNullable(getString(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION)).orElse("'" + HttpRequest.BodyType.STRING + "'");
        this.defaultBodyExpression = getString(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION);
        this.defaultHeadersExpression = getString(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION);
        this.httpRequestMapperIds = Optional.ofNullable(getList(HTTP_REQUEST_MAPPER_IDS)).orElse(Lists.newArrayList());
        this.messageSplitterIds = Optional.ofNullable(getList(MESSAGE_SPLITTER_IDS)).orElse(Lists.newArrayList());
    }


    public String getDefaultBodyExpression() {
        return defaultBodyExpression;
    }

    public String getDefaultBodyTypeExpression() {
        return defaultBodyTypeExpression;
    }

    public String getDefaultHeadersExpression() {
        return defaultHeadersExpression;
    }

    public String getDefaultMethodExpression() {
        return defaultMethodExpression;
    }

    public String getDefaultUrlExpression() {
        return defaultUrlExpression;
    }

    public MapperMode getDefaultRequestMapperMode() {
        return defaultRequestMapperMode;
    }

    public String getQueueName() {
        return queueName;
    }

    public Long getMissingIdCacheTTLSec() {
        return missingIdCacheTTLSec;
    }

    public Long getMissingVersionCacheTTLSec() {
        return missingVersionCacheTTLSec;
    }

    public Long getMissingSchemaCacheTTLSec() {
        return missingSchemaCacheTTLSec;
    }

    public Integer getMissingCacheSize() {
        return missingCacheSize;
    }

    public Integer getBearerAuthCacheExpiryBufferSeconds() {
        return bearerAuthCacheExpiryBufferSeconds;
    }

    public String getBearerAuthScopeClaimName() {
        return bearerAuthScopeClaimName;
    }

    public String getBearerAuthSubClaimName() {
        return bearerAuthSubClaimName;
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

    public PublishMode getPublishMode() {
        return publishMode;
    }

    public String getProducerSuccessTopic() {
        return producerSuccessTopic;
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

    public boolean isMeterRegistryExporterJmxActivate() {
        return meterRegistryExporterJmxActivate;
    }

    public boolean isMeterRegistryExporterPrometheusActivate() {
        return meterRegistryExporterPrometheusActivate;
    }

    public int getMeterRegistryExporterPrometheusPort() {
        return meterRegistryExporterPrometheusPort;
    }

    public boolean isMeterRegistryBindMetricsExecutorService() {
        return meterRegistryBindMetricsExecutorService;
    }

    public boolean isMeterRegistryBindMetricsJvmClassloader() {
        return meterRegistryBindMetricsJvmClassloader;
    }

    public boolean isMeterRegistryBindMetricsJvmProcessor() {
        return meterRegistryBindMetricsJvmProcessor;
    }

    public boolean isMeterRegistryBindMetricsJvmGc() {
        return meterRegistryBindMetricsJvmGc;
    }

    public boolean isMeterRegistryBindMetricsJvmInfo() {
        return meterRegistryBindMetricsJvmInfo;
    }

    public boolean isMeterRegistryBindMetricsJvmMemory() {
        return meterRegistryBindMetricsJvmMemory;
    }

    public boolean isMeterRegistryBindMetricsJvmThread() {
        return meterRegistryBindMetricsJvmThread;
    }

    public boolean isMeterRegistryBindMetricsLogback() {
        return meterRegistryBindMetricsLogback;
    }

    public boolean isMeterRegistryTagIncludeLegacyHost() {
        return meterRegistryTagIncludeLegacyHost;
    }

    public boolean isMeterRegistryTagIncludeUrlPath() {
        return meterRegistryTagIncludeUrlPath;
    }

    public String getHttpClientImplementation() {
        return httpClientImplementation;
    }

    public String getProducerBootstrapServers() {
        return producerBootstrapServers;
    }

    public String getProducerSchemaRegistryUrl() {
        return producerSchemaRegistryUrl;
    }

    public String getProducerErrorTopic() {
        return producerErrorTopic;
    }

    public int getProducerSchemaRegistryCacheCapacity() {
        return producerSchemaRegistryCacheCapacity;
    }

    public boolean isProducerSchemaRegistryautoRegister() {
        return producerSchemaRegistryautoRegister;
    }

    public String isProducerJsonSchemaSpecVersion() {
        return producerJsonSchemaSpecVersion;
    }

    public boolean isProducerJsonWriteDatesAs8601() {
        return producerJsonWriteDatesAs8601;
    }

    public Boolean getProducerJsonIndentOutput() {
        return producerJsonIndentOutput;
    }

    public boolean isProducerJsonOneOfForNullables() {
        return producerJsonOneOfForNullables;
    }

    public boolean isProducerJsonFailInvalidSchema() {
        return producerJsonFailInvalidSchema;
    }

    public boolean isProducerJsonFailUnknownProperties() {
        return producerJsonFailUnknownProperties;
    }

    public String getProducerFormat() {
        return producerFormat;
    }

    public String getProducerContent() {
        return producerContent;
    }

    public String getProducerJsonSchemaSpecVersion() {
        return producerJsonSchemaSpecVersion;
    }


    public String getProducerKeySubjectNameStrategy() {
        return producerKeySubjectNameStrategy;
    }

    public String getProducerValueSubjectNameStrategy() {
        return producerValueSubjectNameStrategy;
    }

    public List<String> getHttpRequestMapperIds() {
        return httpRequestMapperIds;
    }

    public List<String> getMessageSplitterIds() {
        return messageSplitterIds;
    }

    @Override
    public String toString() {
        return "HttpSinkConnectorConfig{" +
                "bearerAuthCacheExpiryBufferSeconds=" + bearerAuthCacheExpiryBufferSeconds +
                ", producerFormat='" + producerFormat + '\'' +
                ", producerBootstrapServers='" + producerBootstrapServers + '\'' +
                ", producerSuccessTopic='" + producerSuccessTopic + '\'' +
                ", producerErrorTopic='" + producerErrorTopic + '\'' +
                ", producerSchemaRegistryUrl='" + producerSchemaRegistryUrl + '\'' +
                ", producerSchemaRegistryCacheCapacity=" + producerSchemaRegistryCacheCapacity +
                ", producerSchemaRegistryautoRegister=" + producerSchemaRegistryautoRegister +
                ", producerJsonSchemaSpecVersion='" + producerJsonSchemaSpecVersion + '\'' +
                ", producerJsonWriteDatesAs8601=" + producerJsonWriteDatesAs8601 +
                ", producerJsonOneOfForNullables=" + producerJsonOneOfForNullables +
                ", producerJsonFailInvalidSchema=" + producerJsonFailInvalidSchema +
                ", producerJsonFailUnknownProperties=" + producerJsonFailUnknownProperties +
                ", producerKeySubjectNameStrategy='" + producerKeySubjectNameStrategy + '\'' +
                ", producerValueSubjectNameStrategy='" + producerValueSubjectNameStrategy + '\'' +
                ", missingIdCacheTTLSec=" + missingIdCacheTTLSec +
                ", missingVersionCacheTTLSec=" + missingVersionCacheTTLSec +
                ", missingSchemaCacheTTLSec=" + missingSchemaCacheTTLSec +
                ", missingCacheSize=" + missingCacheSize +
                ", bearerAuthScopeClaimName='" + bearerAuthScopeClaimName + '\'' +
                ", bearerAuthSubClaimName='" + bearerAuthSubClaimName + '\'' +
                ", meterRegistryExporterJmxActivate=" + meterRegistryExporterJmxActivate +
                ", meterRegistryExporterPrometheusActivate=" + meterRegistryExporterPrometheusActivate +
                ", meterRegistryExporterPrometheusPort=" + meterRegistryExporterPrometheusPort +
                ", meterRegistryBindMetricsExecutorService=" + meterRegistryBindMetricsExecutorService +
                ", meterRegistryBindMetricsJvmClassloader=" + meterRegistryBindMetricsJvmClassloader +
                ", meterRegistryBindMetricsJvmProcessor=" + meterRegistryBindMetricsJvmProcessor +
                ", meterRegistryBindMetricsJvmGc=" + meterRegistryBindMetricsJvmGc +
                ", meterRegistryBindMetricsJvmInfo=" + meterRegistryBindMetricsJvmInfo +
                ", meterRegistryBindMetricsJvmMemory=" + meterRegistryBindMetricsJvmMemory +
                ", meterRegistryBindMetricsJvmThread=" + meterRegistryBindMetricsJvmThread +
                ", meterRegistryBindMetricsLogback=" + meterRegistryBindMetricsLogback +
                ", meterRegistryTagIncludeLegacyHost=" + meterRegistryTagIncludeLegacyHost +
                ", meterRegistryTagIncludeUrlPath=" + meterRegistryTagIncludeUrlPath +
                ", httpClientImplementation='" + httpClientImplementation + '\'' +
                ", defaultSuccessResponseCodeRegex='" + defaultSuccessResponseCodeRegex + '\'' +
                ", defaultRetryResponseCodeRegex='" + defaultRetryResponseCodeRegex + '\'' +
                ", queueName='" + queueName + '\'' +
                ", publishMode=" + publishMode +
                ", defaultRetries=" + defaultRetries +
                ", defaultRetryDelayInMs=" + defaultRetryDelayInMs +
                ", defaultRetryMaxDelayInMs=" + defaultRetryMaxDelayInMs +
                ", defaultRetryDelayFactor=" + defaultRetryDelayFactor +
                ", defaultRetryJitterInMs=" + defaultRetryJitterInMs +
                ", defaultRateLimiterMaxExecutions=" + defaultRateLimiterMaxExecutions +
                ", defaultRateLimiterScope='" + defaultRateLimiterScope + '\'' +
                ", defaultRateLimiterPeriodInMs=" + defaultRateLimiterPeriodInMs +
                ", staticRequestHeaders=" + staticRequestHeaders +
                ", generateMissingRequestId=" + generateMissingRequestId +
                ", generateMissingCorrelationId=" + generateMissingCorrelationId +
                ", maxWaitTimeRegistrationOfQueueConsumerInMs=" + maxWaitTimeRegistrationOfQueueConsumerInMs +
                ", pollDelayRegistrationOfQueueConsumerInMs=" + pollDelayRegistrationOfQueueConsumerInMs +
                ", pollIntervalRegistrationOfQueueConsumerInMs=" + pollIntervalRegistrationOfQueueConsumerInMs +
                ", customFixedThreadpoolSize=" + customFixedThreadpoolSize +
                ", configurationIds=" + configurationIds +
                ", httpRequestMapperIds=" + httpRequestMapperIds +
                ", httpRequestSplitterIds=" + messageSplitterIds +
                ", defaultRequestMapperMode=" + defaultRequestMapperMode +
                ", defaultUrlExpression='" + defaultUrlExpression + '\'' +
                ", defaultMethodExpression='" + defaultMethodExpression + '\'' +
                ", defaultBodyTypeExpression='" + defaultBodyTypeExpression + '\'' +
                ", defaultBodyExpression='" + defaultBodyExpression + '\'' +
                ", defaultHeadersExpression='" + defaultHeadersExpression + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HttpSinkConnectorConfig)) return false;
        if (!super.equals(o)) return false;
        HttpSinkConnectorConfig that = (HttpSinkConnectorConfig) o;
        return producerSchemaRegistryCacheCapacity == that.producerSchemaRegistryCacheCapacity && producerSchemaRegistryautoRegister == that.producerSchemaRegistryautoRegister && producerJsonWriteDatesAs8601 == that.producerJsonWriteDatesAs8601 && producerJsonOneOfForNullables == that.producerJsonOneOfForNullables && producerJsonFailInvalidSchema == that.producerJsonFailInvalidSchema && producerJsonFailUnknownProperties == that.producerJsonFailUnknownProperties && meterRegistryExporterJmxActivate == that.meterRegistryExporterJmxActivate && meterRegistryExporterPrometheusActivate == that.meterRegistryExporterPrometheusActivate && meterRegistryExporterPrometheusPort == that.meterRegistryExporterPrometheusPort && meterRegistryBindMetricsExecutorService == that.meterRegistryBindMetricsExecutorService && meterRegistryBindMetricsJvmClassloader == that.meterRegistryBindMetricsJvmClassloader && meterRegistryBindMetricsJvmProcessor == that.meterRegistryBindMetricsJvmProcessor && meterRegistryBindMetricsJvmGc == that.meterRegistryBindMetricsJvmGc && meterRegistryBindMetricsJvmInfo == that.meterRegistryBindMetricsJvmInfo && meterRegistryBindMetricsJvmMemory == that.meterRegistryBindMetricsJvmMemory && meterRegistryBindMetricsJvmThread == that.meterRegistryBindMetricsJvmThread && meterRegistryBindMetricsLogback == that.meterRegistryBindMetricsLogback && meterRegistryTagIncludeLegacyHost == that.meterRegistryTagIncludeLegacyHost && meterRegistryTagIncludeUrlPath == that.meterRegistryTagIncludeUrlPath && generateMissingRequestId == that.generateMissingRequestId && generateMissingCorrelationId == that.generateMissingCorrelationId && maxWaitTimeRegistrationOfQueueConsumerInMs == that.maxWaitTimeRegistrationOfQueueConsumerInMs && pollDelayRegistrationOfQueueConsumerInMs == that.pollDelayRegistrationOfQueueConsumerInMs && pollIntervalRegistrationOfQueueConsumerInMs == that.pollIntervalRegistrationOfQueueConsumerInMs && Objects.equals(producerFormat, that.producerFormat) && Objects.equals(producerBootstrapServers, that.producerBootstrapServers) && Objects.equals(producerSuccessTopic, that.producerSuccessTopic) && Objects.equals(producerSchemaRegistryUrl, that.producerSchemaRegistryUrl) && Objects.equals(producerJsonSchemaSpecVersion, that.producerJsonSchemaSpecVersion) && Objects.equals(producerKeySubjectNameStrategy, that.producerKeySubjectNameStrategy) && Objects.equals(producerValueSubjectNameStrategy, that.producerValueSubjectNameStrategy) && Objects.equals(missingIdCacheTTLSec, that.missingIdCacheTTLSec) && Objects.equals(missingVersionCacheTTLSec, that.missingVersionCacheTTLSec) && Objects.equals(missingSchemaCacheTTLSec, that.missingSchemaCacheTTLSec) && Objects.equals(missingCacheSize, that.missingCacheSize) && Objects.equals(bearerAuthCacheExpiryBufferSeconds, that.bearerAuthCacheExpiryBufferSeconds) && Objects.equals(bearerAuthScopeClaimName, that.bearerAuthScopeClaimName) && Objects.equals(bearerAuthSubClaimName, that.bearerAuthSubClaimName) && Objects.equals(httpClientImplementation, that.httpClientImplementation) && Objects.equals(defaultSuccessResponseCodeRegex, that.defaultSuccessResponseCodeRegex) && Objects.equals(defaultRetryResponseCodeRegex, that.defaultRetryResponseCodeRegex) && Objects.equals(queueName, that.queueName) && publishMode == that.publishMode && Objects.equals(defaultRetries, that.defaultRetries) && Objects.equals(defaultRetryDelayInMs, that.defaultRetryDelayInMs) && Objects.equals(defaultRetryMaxDelayInMs, that.defaultRetryMaxDelayInMs) && Objects.equals(defaultRetryDelayFactor, that.defaultRetryDelayFactor) && Objects.equals(defaultRetryJitterInMs, that.defaultRetryJitterInMs) && Objects.equals(defaultRateLimiterMaxExecutions, that.defaultRateLimiterMaxExecutions) && Objects.equals(defaultRateLimiterScope, that.defaultRateLimiterScope) && Objects.equals(defaultRateLimiterPeriodInMs, that.defaultRateLimiterPeriodInMs) && Objects.equals(staticRequestHeaders, that.staticRequestHeaders) && Objects.equals(customFixedThreadpoolSize, that.customFixedThreadpoolSize) && Objects.equals(configurationIds, that.configurationIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), producerFormat, producerBootstrapServers, producerSuccessTopic, producerSchemaRegistryUrl, producerSchemaRegistryCacheCapacity, producerSchemaRegistryautoRegister, producerJsonSchemaSpecVersion, producerJsonWriteDatesAs8601, producerJsonOneOfForNullables, producerJsonFailInvalidSchema, producerJsonFailUnknownProperties, producerKeySubjectNameStrategy, producerValueSubjectNameStrategy, missingIdCacheTTLSec, missingVersionCacheTTLSec, missingSchemaCacheTTLSec, missingCacheSize, bearerAuthCacheExpiryBufferSeconds, bearerAuthScopeClaimName, bearerAuthSubClaimName, meterRegistryExporterJmxActivate, meterRegistryExporterPrometheusActivate, meterRegistryExporterPrometheusPort, meterRegistryBindMetricsExecutorService, meterRegistryBindMetricsJvmClassloader, meterRegistryBindMetricsJvmProcessor, meterRegistryBindMetricsJvmGc, meterRegistryBindMetricsJvmInfo, meterRegistryBindMetricsJvmMemory, meterRegistryBindMetricsJvmThread, meterRegistryBindMetricsLogback, meterRegistryTagIncludeLegacyHost, meterRegistryTagIncludeUrlPath, httpClientImplementation, defaultSuccessResponseCodeRegex, defaultRetryResponseCodeRegex, queueName, publishMode, defaultRetries, defaultRetryDelayInMs, defaultRetryMaxDelayInMs, defaultRetryDelayFactor, defaultRetryJitterInMs, defaultRateLimiterMaxExecutions, defaultRateLimiterScope, defaultRateLimiterPeriodInMs, staticRequestHeaders, generateMissingRequestId, generateMissingCorrelationId, maxWaitTimeRegistrationOfQueueConsumerInMs, pollDelayRegistrationOfQueueConsumerInMs, pollIntervalRegistrationOfQueueConsumerInMs, customFixedThreadpoolSize, configurationIds);
    }
}
