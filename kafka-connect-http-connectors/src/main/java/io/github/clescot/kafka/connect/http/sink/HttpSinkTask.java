package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpExchangeSerializer;
import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaJsonSerializerConfig.WRITE_DATES_AS_ISO8601;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES;


public class HttpSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private static final List<String> JSON_SCHEMA_VERSIONS = Lists.newArrayList("draft_4","draft_6","draft_7","draft_2019_09");
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

    private ErrantRecordReporter errantRecordReporter;
    private HttpTask<SinkRecord> httpTask;
    private final KafkaProducer<String, HttpExchange> producer;
    private String queueName;
    private Queue<KafkaRecord> queue;
    private PublishMode publishMode;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private Map<String, Object> producerSettings;

    public HttpSinkTask() {
        producer = new KafkaProducer<>();
    }

    /**
     * for tests only.
     *
     * @param mock true mock the underlying producer, false not.
     */
    protected HttpSinkTask(boolean mock) {
        producer = new KafkaProducer<>(mock);
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    /**
     * @param settings configure the connector
     */
    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings cannot be null");
        this.httpSinkConnectorConfig = new HttpSinkConnectorConfig(HttpSinkConfigDefinition.config(), settings);

        httpTask = new HttpTask<>(httpSinkConnectorConfig);

        try {
            errantRecordReporter = context.errantRecordReporter();
            if (errantRecordReporter == null) {
                LOGGER.warn("Dead Letter Queue (DLQ) is not enabled. it is recommended to configure a Dead Letter Queue for a better error handling.");
            }
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            errantRecordReporter = null;
        }


        this.publishMode = httpSinkConnectorConfig.getPublishMode();
        LOGGER.debug("publishMode: {}", publishMode);

        switch (publishMode) {
            case PRODUCER:
                //low-level producer is configured (bootstrap.servers is a requirement)
                Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerBootstrapServers()), "producer.bootstrap.servers is not set.\n" + httpSinkConnectorConfig.toString());
                Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerTopic()), "producer.topic is not set.\n" + httpSinkConnectorConfig.toString());
                Serializer<HttpExchange> serializer = getHttpExchangeSerializer(httpSinkConnectorConfig);
                producerSettings = httpSinkConnectorConfig.originalsWithPrefix(PRODUCER_PREFIX);
                producer.configure(producerSettings, new StringSerializer(), serializer);

                //connectivity check for producer
                checkKafkaConnectivity();

                break;
            case IN_MEMORY_QUEUE:
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
                break;
            case DLQ:
                //TODO DLQ publish mode
                LOGGER.debug("DLQ publish mode");
                break;
            case NONE:
            default:
                LOGGER.debug("NONE publish mode");
        }


    }

    private void checkKafkaConnectivity() {
        LOGGER.info("test connectivity to kafka cluster for producer with address :'{}' for topic:'{}'", httpSinkConnectorConfig.getProducerBootstrapServers(), httpSinkConnectorConfig.getProducerTopic());
        List<PartitionInfo> partitionInfos;
        try {
            partitionInfos = producer.partitionsFor(httpSinkConnectorConfig.getProducerTopic());
        } catch (KafkaException e) {
            LOGGER.error("connectivity error.\nproducer settings :");
            for (Map.Entry<String, Object> entry : producerSettings.entrySet()) {
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
        LOGGER.info("producer format:'{}'",format);
        //if format is json
        if (JSON.equalsIgnoreCase(format)) {
            //json schema serde config
            Map<String, Object> serdeConfig = Maps.newHashMap();

            String schemaRegistryUrl = httpSinkConnectorConfig.getProducerSchemaRegistryUrl();
            serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

            int schemaRegistryCacheCapacity = httpSinkConnectorConfig.getProducerSchemaRegistrycacheCapacity();
            List<SchemaProvider> schemaProviders = Lists.newArrayList();
            schemaProviders.add(new JsonSchemaProvider());
            Map<String,Object> config = Maps.newHashMap();
            if(httpSinkConnectorConfig.getMissingIdCacheTTLSec()!=null){
                config.put(MISSING_ID_CACHE_TTL_SEC,httpSinkConnectorConfig.getMissingIdCacheTTLSec());
            }
            if(httpSinkConnectorConfig.getMissingVersionCacheTTLSec()!=null){
                config.put(MISSING_VERSION_CACHE_TTL_SEC,httpSinkConnectorConfig.getMissingVersionCacheTTLSec());
            }
            if(httpSinkConnectorConfig.getMissingSchemaCacheTTLSec()!=null){
                config.put(MISSING_SCHEMA_CACHE_TTL_SEC,httpSinkConnectorConfig.getMissingVersionCacheTTLSec());
            }
            if(httpSinkConnectorConfig.getMissingCacheSize()!=null){
                config.put(MISSING_CACHE_SIZE,httpSinkConnectorConfig.getMissingCacheSize());
            }
            if(httpSinkConnectorConfig.getMissingCacheSize()!=null){
                config.put(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS,httpSinkConnectorConfig.getBearerAuthCacheExpiryBufferSeconds());
            }
            if(httpSinkConnectorConfig.getBearerAuthScopeClaimName()!=null){
                config.put(BEARER_AUTH_SCOPE_CLAIM_NAME,httpSinkConnectorConfig.getBearerAuthScopeClaimName());
            }
            if(httpSinkConnectorConfig.getBearerAuthSubClaimName()!=null){
                config.put(BEARER_AUTH_SUB_CLAIM_NAME,httpSinkConnectorConfig.getBearerAuthSubClaimName());
            }
            Map<String,String> httpHeaders = Maps.newHashMap();
            RestService restService = new RestService(schemaRegistryUrl);
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, schemaRegistryCacheCapacity,schemaProviders,config,httpHeaders);

            boolean autoRegisterSchemas = httpSinkConnectorConfig.isProducerSchemaRegistryautoRegister();
            serdeConfig.put(AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'autoRegisterSchemas':'{}'",autoRegisterSchemas);

            String jsonSchemaSpecVersion = httpSinkConnectorConfig.isProducerJsonSchemaSpecVersion();
            Preconditions.checkNotNull(jsonSchemaSpecVersion);
            Preconditions.checkArgument(!jsonSchemaSpecVersion.isEmpty(),"'jsonSchemaSpecVersion' must not be an empty string");
            Preconditions.checkArgument(JSON_SCHEMA_VERSIONS.contains(jsonSchemaSpecVersion.toLowerCase()),"jsonSchemaSpecVersion supported values are 'draft_4','draft_6','draft_7','draft_2019_09' but not '"+jsonSchemaSpecVersion+"'");
            serdeConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecVersion);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'jsonSchemaSpecVersion':'{}'",jsonSchemaSpecVersion);

            boolean writeDatesAsIso8601 = httpSinkConnectorConfig.isProducerJsonWriteDatesAs8601();
            serdeConfig.put(WRITE_DATES_AS_ISO8601, writeDatesAsIso8601);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'writeDatesAsIso8601':'{}'",writeDatesAsIso8601);

            boolean oneOfForNullables = httpSinkConnectorConfig.isProducerJsonOneOfForNullables();
            serdeConfig.put(ONEOF_FOR_NULLABLES, oneOfForNullables);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'oneOfForNullables':'{}'",oneOfForNullables);

            boolean failInvalidSchema = httpSinkConnectorConfig.isProducerJsonFailInvalidSchema();
            serdeConfig.put(FAIL_INVALID_SCHEMA, failInvalidSchema);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'failInvalidSchema':'{}'",failInvalidSchema);

            boolean failUnknownProperties = httpSinkConnectorConfig.isProducerJsonFailUnknownProperties();
            serdeConfig.put(FAIL_UNKNOWN_PROPERTIES, failUnknownProperties);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'failUnknownProperties':'{}'",failUnknownProperties);

            serdeConfig.put("key.subject.name.strategy",httpSinkConnectorConfig.getProducerKeySubjectNameStrategy());
            serdeConfig.put("value.subject.name.strategy",httpSinkConnectorConfig.getProducerValueSubjectNameStrategy());

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
        List<CompletableFuture<HttpExchange>> completableFutures = records.stream().map(this::process).collect(Collectors.toList());
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());
        LOGGER.debug("HttpExchanges created :'{}'", httpExchanges.size());

    }

    private CompletableFuture<HttpExchange> process(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        Class<?> valueClass = value.getClass();
        LOGGER.debug("valueClass is '{}'", valueClass.getName());
        LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
        try {
            if (sinkRecord.value() == null) {
                throw new ConnectException("sinkRecord Value is null :" + sinkRecord);
            }
            return httpTask.processRecord(sinkRecord)
                    .thenApply(
                            httpExchange -> {
                                //publish eventually to 'in memory' queue
                                if (PublishMode.IN_MEMORY_QUEUE.equals(this.publishMode)) {
                                    LOGGER.debug("publish.mode : 'IN_MEMORY_QUEUE': http exchange published to queue '{}':{}", queueName, httpExchange);
                                    queue.offer(new KafkaRecord(sinkRecord.headers(), sinkRecord.keySchema(), sinkRecord.key(), httpExchange));
                                } else if (PublishMode.DLQ.equals(this.publishMode)) {
                                    LOGGER.debug("publish.mode : 'DLQ' : HttpExchange published to DLQ");
                                    SinkRecord myRecord = new SinkRecord(
                                            sinkRecord.topic(),
                                            sinkRecord.kafkaPartition(),
                                            sinkRecord.keySchema(),
                                            sinkRecord.key(),
                                            new ConnectSchema(Schema.Type.STRUCT),
                                            httpExchange,
                                            sinkRecord.kafkaOffset(),
                                            sinkRecord.timestamp(),
                                            sinkRecord.timestampType());
                                    errantRecordReporter.report(myRecord, new FakeErrantRecordReporterException());
                                } else if (PublishMode.PRODUCER.equals(this.publishMode)) {

                                    LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange will be published at topic : '{}'", httpSinkConnectorConfig.getProducerTopic());
                                    try {
                                        ProducerRecord<String, HttpExchange> myRecord = new ProducerRecord<>(httpSinkConnectorConfig.getProducerTopic(), httpExchange);
                                        LOGGER.trace("before send to {}", httpSinkConnectorConfig.getProducerTopic());
                                        this.producer.send(myRecord).get(3, TimeUnit.SECONDS);
                                        LOGGER.debug("✉✉ record sent ✉✉");
                                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                        LOGGER.debug("/!\\ ☠☠ record NOT sent ☠☠");
                                        LOGGER.error(e.getMessage());
                                        throw new ConnectException(e);
                                    }
                                } else {
                                    LOGGER.debug("publish.mode : 'NONE' http exchange NOT published :'{}'", httpExchange);
                                }
                                return httpExchange;
                            }
                    );
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
                    return CompletableFuture.failedFuture(e);
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

    public Configuration getDefaultConfiguration() {
        return httpTask.getDefaultConfiguration();
    }

    public HttpTask<SinkRecord> getHttpTask() {
        return httpTask;
    }
}
