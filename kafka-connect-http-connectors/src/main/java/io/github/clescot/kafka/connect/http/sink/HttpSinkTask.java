package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.github.clescot.kafka.connect.http.HttpExchangeSerdeFactory;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpExchangeSerializer;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.sink.mapper.DirectHttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.mapper.HttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.mapper.JEXLHttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.mapper.MapperMode;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaJsonSerializerConfig.WRITE_DATES_AS_ISO8601;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;


public abstract class HttpSinkTask<R, S> extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private static final List<String> JSON_SCHEMA_VERSIONS = Lists.newArrayList("draft_4", "draft_6", "draft_7", "draft_2019_09");
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    public static final String PRODUCER_PREFIX = "producer.";
    public static final String JSON = "json";
    public static final String BEARER_AUTH_SUB_CLAIM_NAME = "bearer.auth.sub.claim.name";
    public static final String BEARER_AUTH_SCOPE_CLAIM_NAME = "bearer.auth.scope.claim.name";
    public static final String BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS = "bearer.auth.cache.expiry.buffer.seconds";
    public static final String MISSING_CACHE_SIZE = "missing.cache.size";
    public static final String MISSING_SCHEMA_CACHE_TTL_SEC = "missing.schema.cache.ttl.sec";
    public static final String MISSING_VERSION_CACHE_TTL_SEC = "missing.version.cache.ttl.sec";
    public static final String MISSING_ID_CACHE_TTL_SEC = "missing.id.cache.ttl.sec";
    public static final String RECORD_NOT_SENT = "/!\\ ☠☠ record NOT sent ☠☠";
    public static final String JEXL_ALWAYS_MATCHES = "true";
    private Configuration<R, S> defaultConfiguration;
    private List<Configuration<R, S>> customConfigurations;
    private HttpRequestMapper defaultHttpRequestMapper;
    private List<HttpRequestMapper> httpRequestMappers;
    public static final String DEFAULT_CONFIGURATION_ID = "default";
    public static final String DEFAULT_MAPPER_ID = "default";
    private final HttpClientFactory<R, S> httpClientFactory;

    private ErrantRecordReporter errantRecordReporter;
    private HttpTask<SinkRecord, R, S> httpTask;
    private final KafkaProducer<String, HttpExchange> producer;
    private String queueName;
    private Queue<KafkaRecord> queue;
    private PublishMode publishMode;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private Map<String, Object> producerSettings;
    private static CompositeMeterRegistry meterRegistry;
    private ExecutorService executorService;

    public HttpSinkTask(HttpClientFactory<R, S> httpClientFactory) {
        this.httpClientFactory = httpClientFactory;
        producer = new KafkaProducer<>();
    }

    /**
     * for tests only.
     *
     * @param mock true mock the underlying producer, false not.
     */
    protected HttpSinkTask(HttpClientFactory<R, S> httpClientFactory, boolean mock) {
        this.httpClientFactory = httpClientFactory;
        producer = new KafkaProducer<>(mock);
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    private List<Configuration<R, S>> buildCustomConfigurations(HttpClientFactory<R, S> httpClientFactory,
                                                                AbstractConfig config,
                                                                Configuration<R, S> defaultConfiguration,
                                                                ExecutorService executorService) {
        CopyOnWriteArrayList<Configuration<R, S>> configurations = Lists.newCopyOnWriteArrayList();

        for (String configId : Optional.ofNullable(config.getList(CONFIGURATION_IDS)).orElse(Lists.newArrayList())) {
            Configuration<R, S> configuration = new Configuration<>(configId, httpClientFactory, config, executorService, meterRegistry);
            if (configuration.getHttpClient() == null) {
                configuration.setHttpClient(defaultConfiguration.getHttpClient());
            }

            //we reuse the default retry policy if not set
            Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = defaultConfiguration.getRetryPolicy();
            if (configuration.getRetryPolicy().isEmpty() && defaultRetryPolicy.isPresent()) {
                configuration.setRetryPolicy(defaultRetryPolicy.get());
            }
            //we reuse the default success response code regex if not set
            configuration.setSuccessResponseCodeRegex(defaultConfiguration.getSuccessResponseCodeRegex());

            Optional<Pattern> defaultRetryResponseCodeRegex = defaultConfiguration.getRetryResponseCodeRegex();
            if (configuration.getRetryResponseCodeRegex().isEmpty() && defaultRetryResponseCodeRegex.isPresent()) {
                configuration.setRetryResponseCodeRegex(defaultRetryResponseCodeRegex.get());
            }

            configurations.add(configuration);
        }
        return configurations;
    }

    private List<HttpRequestMapper> buildCustomHttpRequestMappers(AbstractConfig config, JexlEngine jexlEngine) {
        List<HttpRequestMapper> requestMappers = Lists.newArrayList();
        for (String httpRequestMapperId : Optional.ofNullable(config.getList(HTTP_REQUEST_MAPPER_IDS)).orElse(Lists.newArrayList())) {
            HttpRequestMapper httpRequestMapper;
            String prefix = "http.request.mapper." + httpRequestMapperId;
            Map<String, Object> settings = config.originalsWithPrefix(prefix);
            String modeKey = ".mode";
            MapperMode mapperMode = MapperMode.valueOf(Optional.ofNullable(settings.get(modeKey)).orElse(MapperMode.DIRECT.name()).toString());
            switch (mapperMode) {
                case JEXL: {
                    httpRequestMapper = new JEXLHttpRequestMapper(jexlEngine,
                            (String) settings.get(".matcher"),
                            (String) settings.get(".url"),
                            (String) settings.get(".method"),
                            (String) settings.get(".bodytype"),
                            (String) settings.get(".body"),
                            (String) settings.get(".headers")
                    );
                    break;
                }
                case DIRECT:
                default: {
                    httpRequestMapper = new DirectHttpRequestMapper(jexlEngine, (String) settings.get(".matcher"));
                    break;
                }
            }
            requestMappers.add(httpRequestMapper);
        }
        return requestMappers;
    }


    /**
     * @param settings configure the connector
     */
    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings cannot be null");
        HttpSinkConfigDefinition httpSinkConfigDefinition = new HttpSinkConfigDefinition(settings);
        this.httpSinkConnectorConfig = new HttpSinkConnectorConfig(httpSinkConfigDefinition.config(), settings);


        //build executorService
        Optional<Integer> customFixedThreadPoolSize = Optional.ofNullable(httpSinkConnectorConfig.getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE));
        customFixedThreadPoolSize.ifPresent(integer -> this.executorService = buildExecutorService(integer));

        //build meterRegistry
        HttpSinkTask.meterRegistry = buildMeterRegistry(httpSinkConnectorConfig);

        //build httpRequestMappers

        // Restricted permissions to a safe set but with URI allowed
        JexlPermissions permissions = new JexlPermissions.ClassPermissions(SinkRecord.class, ConnectRecord.class, HttpRequest.class);
        // Create the engine
        JexlFeatures features = new JexlFeatures()
                .loops(false)
                .sideEffectGlobal(false)
                .sideEffect(false);
        JexlEngine jexlEngine = new JexlBuilder().features(features).permissions(permissions).create();

        MapperMode defaultRequestMapperMode = httpSinkConnectorConfig.getDefaultRequestMapperMode();
        switch (defaultRequestMapperMode) {
            case JEXL: {
                Preconditions.checkNotNull(httpSinkConnectorConfig.getDefaultUrlExpression(), "'" + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION + "' need to be set");
                this.defaultHttpRequestMapper = new JEXLHttpRequestMapper(jexlEngine,
                        JEXL_ALWAYS_MATCHES,
                        httpSinkConnectorConfig.getDefaultUrlExpression(),
                        httpSinkConnectorConfig.getDefaultMethodExpression(),
                        httpSinkConnectorConfig.getDefaultBodyTypeExpression(),
                        httpSinkConnectorConfig.getDefaultBodyExpression(),
                        httpSinkConnectorConfig.getDefaultHeadersExpression()
                );
                break;
            }
            case DIRECT:
            default: {
                this.defaultHttpRequestMapper = new DirectHttpRequestMapper(jexlEngine, JEXL_ALWAYS_MATCHES);
                break;
            }
        }
        this.httpRequestMappers = buildCustomHttpRequestMappers(httpSinkConnectorConfig, jexlEngine);

        //configurations
        this.defaultConfiguration = new Configuration<>(DEFAULT_CONFIGURATION_ID, httpClientFactory, httpSinkConnectorConfig, executorService, meterRegistry);
        customConfigurations = buildCustomConfigurations(httpClientFactory, httpSinkConnectorConfig, defaultConfiguration, executorService);

        httpTask = new HttpTask<>(httpSinkConnectorConfig, defaultConfiguration, customConfigurations, meterRegistry, executorService);

        try {
            errantRecordReporter = context.errantRecordReporter();
            if (errantRecordReporter == null) {
                LOGGER.warn("Dead Letter Queue (DLQ) is not enabled. it is recommended to configure a Dead Letter Queue for a better error handling.");
            }
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            errantRecordReporter = null;
        }

        //configure publishMode
        this.publishMode = httpSinkConnectorConfig.getPublishMode();
        LOGGER.debug("publishMode: {}", publishMode);

        switch (publishMode) {
            case PRODUCER:
                configureProducerPublishMode();

                break;
            case IN_MEMORY_QUEUE:
                configureInMemoryQueue();
                break;
            case NONE:
            default:
                LOGGER.debug("NONE publish mode");
        }


    }

    private void configureInMemoryQueue() {
        this.queueName = Optional.ofNullable(httpSinkConnectorConfig.getString(ConfigConstants.QUEUE_NAME)).orElse(QueueFactory.DEFAULT_QUEUE_NAME);
        this.queue = QueueFactory.getQueue(queueName);
        this.queueName = httpSinkConnectorConfig.getQueueName();
        Preconditions.checkArgument(QueueFactory.hasAConsumer(
                queueName,
                httpSinkConnectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs()
                , httpSinkConnectorConfig.getPollDelayRegistrationOfQueueConsumerInMs(),
                httpSinkConnectorConfig.getPollIntervalRegistrationOfQueueConsumerInMs()
        ), "timeout : '" + httpSinkConnectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs() +
                "'ms timeout reached :" + queueName + "' queue hasn't got any consumer, " +
                "i.e no Source Connector has been configured to consume records published in this in memory queue. " +
                "we stop the Sink Connector to prevent any OutOfMemoryError.");
    }

    private void configureProducerPublishMode() {
        //low-level producer is configured (bootstrap.servers is a requirement)
        Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerBootstrapServers()), "producer.bootstrap.servers is not set.\n" + httpSinkConnectorConfig.toString());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerSuccessTopic()), "producer.success.topic is not set.\n" + httpSinkConnectorConfig.toString());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerErrorTopic()), "producer.error.topic is not set.\n" + httpSinkConnectorConfig.toString());
        Serializer<HttpExchange> serializer = getHttpExchangeSerializer(httpSinkConnectorConfig);
        producerSettings = httpSinkConnectorConfig.originalsWithPrefix(PRODUCER_PREFIX);
        producer.configure(producerSettings, new StringSerializer(), serializer);

        //connectivity check for producer
        checkKafkaConnectivity(httpSinkConnectorConfig,producer);
    }

    /**
     * define a static field from a non-static method need a static synchronized method
     *
     * @param customFixedThreadPoolSize max thread pool size for the executorService.
     * @return executorService
     */
    public ExecutorService buildExecutorService(Integer customFixedThreadPoolSize) {
        return Executors.newFixedThreadPool(customFixedThreadPoolSize);
    }

    private CompositeMeterRegistry buildMeterRegistry(AbstractConfig config) {
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        boolean activateJMX = Boolean.parseBoolean(config.getString(METER_REGISTRY_EXPORTER_JMX_ACTIVATE));
        if (activateJMX) {
            JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
            jmxMeterRegistry.start();
            compositeMeterRegistry.add(jmxMeterRegistry);
        }
        boolean activatePrometheus = Boolean.parseBoolean(config.getString(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE));
        if (activatePrometheus) {
            PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Integer prometheusPort = config.getInt(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT);
            // you can set the daemon flag to false if you want the server to block

            try {
                int port = prometheusPort != null ? prometheusPort : 9090;
                PrometheusRegistry prometheusRegistry = prometheusMeterRegistry.getPrometheusRegistry();
                HTTPServer.builder()
                        .port(port)
                        .registry(prometheusRegistry)
                        .buildAndStart();
            } catch (IOException e) {
                throw new HttpException(e);
            }
            compositeMeterRegistry.add(prometheusMeterRegistry);
        }
        return compositeMeterRegistry;
    }

    private void checkKafkaConnectivity(HttpSinkConnectorConfig sinkConnectorConfig, KafkaProducer<String, HttpExchange> producer) {
        LOGGER.info("test connectivity to kafka cluster for producer with address :'{}' for topic:'{}'", sinkConnectorConfig.getProducerBootstrapServers(), sinkConnectorConfig.getProducerSuccessTopic());
        List<PartitionInfo> partitionInfos;
        try {
            partitionInfos = producer.partitionsFor(sinkConnectorConfig.getProducerSuccessTopic());
        } catch (KafkaException e) {
            LOGGER.error("connectivity error.\nproducer settings :");
            for (Map.Entry<String, Object> entry : sinkConnectorConfig.originalsWithPrefix(PRODUCER_PREFIX).entrySet()) {
                LOGGER.error("   '{}':'{}'", entry.getKey(), entry.getValue());
            }
            LOGGER.error("connectivity error :{}", e.getMessage());
            throw e;
        }
        if (partitionInfos.isEmpty()) {
            LOGGER.error("connectivity error");
            throw new IllegalStateException("no partitionInfo can be get. connectivity error.");
        } else {
            LOGGER.info("connectivity OK");
        }
    }

    private Serializer<HttpExchange> getHttpExchangeSerializer(HttpSinkConnectorConfig httpSinkConnectorConfig) {
        Serializer<HttpExchange> serializer;
        String format = httpSinkConnectorConfig.getProducerFormat();
        LOGGER.info("producer format:'{}'", format);
        //if format is json
        if (JSON.equalsIgnoreCase(format)) {
            //json schema serde config
            Map<String, Object> serdeConfig = Maps.newHashMap();

            String schemaRegistryUrl = httpSinkConnectorConfig.getProducerSchemaRegistryUrl();
            serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

            int schemaRegistryCacheCapacity = httpSinkConnectorConfig.getProducerSchemaRegistryCacheCapacity();
            List<SchemaProvider> schemaProviders = Lists.newArrayList();
            schemaProviders.add(new JsonSchemaProvider());
            Map<String, Object> config = Maps.newHashMap();
            if (httpSinkConnectorConfig.getMissingIdCacheTTLSec() != null) {
                config.put(MISSING_ID_CACHE_TTL_SEC, httpSinkConnectorConfig.getMissingIdCacheTTLSec());
            }
            if (httpSinkConnectorConfig.getMissingVersionCacheTTLSec() != null) {
                config.put(MISSING_VERSION_CACHE_TTL_SEC, httpSinkConnectorConfig.getMissingVersionCacheTTLSec());
            }
            if (httpSinkConnectorConfig.getMissingSchemaCacheTTLSec() != null) {
                config.put(MISSING_SCHEMA_CACHE_TTL_SEC, httpSinkConnectorConfig.getMissingVersionCacheTTLSec());
            }
            if (httpSinkConnectorConfig.getMissingCacheSize() != null) {
                config.put(MISSING_CACHE_SIZE, httpSinkConnectorConfig.getMissingCacheSize());
            }
            if (httpSinkConnectorConfig.getMissingCacheSize() != null) {
                config.put(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS, httpSinkConnectorConfig.getBearerAuthCacheExpiryBufferSeconds());
            }
            if (httpSinkConnectorConfig.getBearerAuthScopeClaimName() != null) {
                config.put(BEARER_AUTH_SCOPE_CLAIM_NAME, httpSinkConnectorConfig.getBearerAuthScopeClaimName());
            }
            if (httpSinkConnectorConfig.getBearerAuthSubClaimName() != null) {
                config.put(BEARER_AUTH_SUB_CLAIM_NAME, httpSinkConnectorConfig.getBearerAuthSubClaimName());
            }
            Map<String, String> httpHeaders = Maps.newHashMap();
            RestService restService = new RestService(schemaRegistryUrl);
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, schemaRegistryCacheCapacity, schemaProviders, config, httpHeaders);

            boolean autoRegisterSchemas = httpSinkConnectorConfig.isProducerSchemaRegistryautoRegister();
            serdeConfig.put(AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'autoRegisterSchemas':'{}'", autoRegisterSchemas);

            String jsonSchemaSpecVersion = httpSinkConnectorConfig.isProducerJsonSchemaSpecVersion();
            Preconditions.checkNotNull(jsonSchemaSpecVersion);
            Preconditions.checkArgument(!jsonSchemaSpecVersion.isEmpty(), "'jsonSchemaSpecVersion' must not be an empty string");
            Preconditions.checkArgument(JSON_SCHEMA_VERSIONS.contains(jsonSchemaSpecVersion.toLowerCase()), "jsonSchemaSpecVersion supported values are 'draft_4','draft_6','draft_7','draft_2019_09' but not '" + jsonSchemaSpecVersion + "'");
            serdeConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecVersion);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'jsonSchemaSpecVersion':'{}'", jsonSchemaSpecVersion);

            boolean writeDatesAsIso8601 = httpSinkConnectorConfig.isProducerJsonWriteDatesAs8601();
            serdeConfig.put(WRITE_DATES_AS_ISO8601, writeDatesAsIso8601);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'writeDatesAsIso8601':'{}'", writeDatesAsIso8601);

            boolean oneOfForNullables = httpSinkConnectorConfig.isProducerJsonOneOfForNullables();
            serdeConfig.put(ONEOF_FOR_NULLABLES, oneOfForNullables);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'oneOfForNullables':'{}'", oneOfForNullables);

            boolean failInvalidSchema = httpSinkConnectorConfig.isProducerJsonFailInvalidSchema();
            serdeConfig.put(FAIL_INVALID_SCHEMA, failInvalidSchema);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'failInvalidSchema':'{}'", failInvalidSchema);

            boolean failUnknownProperties = httpSinkConnectorConfig.isProducerJsonFailUnknownProperties();
            serdeConfig.put(FAIL_UNKNOWN_PROPERTIES, failUnknownProperties);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'failUnknownProperties':'{}'", failUnknownProperties);

            serdeConfig.put("key.subject.name.strategy", httpSinkConnectorConfig.getProducerKeySubjectNameStrategy());
            serdeConfig.put("value.subject.name.strategy", httpSinkConnectorConfig.getProducerValueSubjectNameStrategy());

            HttpExchangeSerdeFactory httpExchangeSerdeFactory = new HttpExchangeSerdeFactory(schemaRegistryClient, serdeConfig);
            serializer = httpExchangeSerdeFactory.buildValueSerde().serializer();
        } else {
            //serialize as a simple string
            serializer = new HttpExchangeSerializer();
        }
        return serializer;
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        Preconditions.checkNotNull(records, "records collection to be processed is null");
        if (records.isEmpty()) {
            LOGGER.debug("no records");
            return;
        }
        Preconditions.checkNotNull(httpTask, "httpTask is null. 'start' method must be called once before put");
        //we submit futures to the pool
        Stream<SinkRecord> stream = records.stream();

        //TODO regroup messages into one https://github.com/clescot/kafka-connect-http/issues/336
        //List<SinkRecord>-> SinkRecord
        List<CompletableFuture<HttpExchange>> completableFutures = stream.map(this::process).collect(Collectors.toList()).stream().flatMap(list -> list.stream()).collect(Collectors.toList());
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());
        LOGGER.debug("HttpExchanges created :'{}'", httpExchanges.size());

    }

    private List<CompletableFuture<HttpExchange>> process(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        Class<?> valueClass = value.getClass();
        LOGGER.debug("valueClass is '{}'", valueClass.getName());
        LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
        try {
            if (sinkRecord.value() == null) {
                throw new ConnectException("sinkRecord Value is null :" + sinkRecord);
            }

            //httpRequestMapper
            HttpRequestMapper httpRequestMapper = httpRequestMappers.stream()
                    .filter(mapper -> mapper.matches(sinkRecord))
                    .findFirst()
                    .orElse(defaultHttpRequestMapper);

            //build HttpRequest
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            List<HttpRequest> httpRequests = Lists.newArrayList();

            //splitter
            if (HttpRequest.BodyType.STRING.equals(httpRequest.getBodyType())
                    &&httpRequestMapper.getSplitPattern()!=null) {
                String bodyAsString = httpRequest.getBodyAsString();
                httpRequests.addAll(Lists.newArrayList(httpRequestMapper.getSplitPattern().split(bodyAsString, httpRequestMapper.getSplitLimit())).stream()
                        .map(part -> {
                            HttpRequest partRequest = new HttpRequest(httpRequest);
                            partRequest.setBodyAsString(part);
                            return partRequest;
                        })
                        .collect(Collectors.toList()));
            } else {
                //no splitter
                httpRequests.add(httpRequest);
            }
            //we don't simulate some sub-sinkRecord, to preserve the integrity of the commit notion

            return httpRequests
                    .stream()
                    .map(currentRequest -> httpTask
                            .processHttpRequest(currentRequest)
                            .thenApply(publish(sinkRecord)))
                    .collect(Collectors.toList());

        } catch (ConnectException connectException) {
            LOGGER.error("sink value class is '{}'", valueClass.getName());
            if (errantRecordReporter != null) {
                errantRecordReporter.report(sinkRecord, connectException);
            } else {
                LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            }
            throw connectException;
        } catch (Exception e) {
            if (errantRecordReporter != null) {
                // Send errant record to error reporter
                Future<Void> future = errantRecordReporter.report(sinkRecord, e);
                // Optionally wait till the failure's been recorded in Kafka
                try {
                    future.get();
                    return List.of(CompletableFuture.failedFuture(e));
                } catch (InterruptedException | ExecutionException ex) {
                    Thread.currentThread().interrupt();
                    throw new ConnectException(ex);
                }
            } else {
                // There's no error reporter, so fail
                throw new ConnectException("Failed on record", e);
            }
        }

    }

    private @NotNull Function<HttpExchange, HttpExchange> publish(SinkRecord sinkRecord) {
        return httpExchange -> {
            //publish eventually to 'in memory' queue
            if (PublishMode.IN_MEMORY_QUEUE.equals(this.publishMode)) {
                LOGGER.debug("publish.mode : 'IN_MEMORY_QUEUE': http exchange published to queue '{}':{}", queueName, httpExchange);
                boolean offer = queue.offer(new KafkaRecord(sinkRecord.headers(), sinkRecord.keySchema(), sinkRecord.key(), httpExchange));
                if (!offer) {
                    LOGGER.error("sinkRecord(topic:{},partition:{},key:{},timestamp:{}) not added to the 'in memory' queue:{}",
                            sinkRecord.topic(),
                            sinkRecord.kafkaPartition(),
                            sinkRecord.key(),
                            sinkRecord.timestamp(),
                            queueName
                    );
                }
            } else if (PublishMode.PRODUCER.equals(this.publishMode)) {

                LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange success will be published at topic : '{}'", httpSinkConnectorConfig.getProducerSuccessTopic());
                LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange error will be published at topic : '{}'", httpSinkConnectorConfig.getProducerErrorTopic());
                try {
                    String targetTopic = httpExchange.isSuccess() ? httpSinkConnectorConfig.getProducerSuccessTopic() : httpSinkConnectorConfig.getProducerErrorTopic();
                    ProducerRecord<String, HttpExchange> myRecord = new ProducerRecord<>(targetTopic, httpExchange);
                    LOGGER.trace("before send to {}", targetTopic);
                    RecordMetadata recordMetadata = this.producer.send(myRecord).get(3, TimeUnit.SECONDS);
                    long offset = recordMetadata.offset();
                    int partition = recordMetadata.partition();
                    long timestamp = recordMetadata.timestamp();
                    String topic = recordMetadata.topic();
                    LOGGER.debug("✉✉ record sent ✉✉ : topic:{},partition:{},offset:{},timestamp:{}", topic, partition, offset, timestamp);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    LOGGER.debug(RECORD_NOT_SENT);
                    LOGGER.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                    throw new ConnectException(RECORD_NOT_SENT, e);
                }
            } else {
                LOGGER.debug("publish.mode : 'NONE' http exchange NOT published :'{}'", httpExchange);
            }
            return httpExchange;
        };
    }

    @Override
    public void stop() {
        if (httpTask == null) {
            LOGGER.error("httpTask hasn't been created with the 'start' method");
            return;
        }
        ExecutorService executorService = httpTask.getExecutorService();
        if (executorService != null) {
            if (!executorService.isShutdown()) {
                executorService.shutdown();
            }
            try {
                boolean awaitTermination = executorService.awaitTermination(30, TimeUnit.SECONDS);
                if (!awaitTermination) {
                    LOGGER.warn("timeout elapsed before executor termination");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException(e);
            }
            LOGGER.info("executor is shutdown : '{}'", executorService.isShutdown());
            LOGGER.info("executor tasks are terminated : '{}'", executorService.isTerminated());
        }
    }

    protected void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }

    public Configuration<R, S> getDefaultConfiguration() {
        Preconditions.checkNotNull(httpTask, "httpTask has not been initialized in the start method");
        return httpTask.getDefaultConfiguration();
    }

    public HttpTask<SinkRecord, R, S> getHttpTask() {
        return httpTask;
    }


    protected HttpRequestMapper getDefaultHttpRequestMapper() {
        return defaultHttpRequestMapper;
    }
}
