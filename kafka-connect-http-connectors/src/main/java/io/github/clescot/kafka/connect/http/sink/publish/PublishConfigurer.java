package io.github.clescot.kafka.connect.http.sink.publish;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.serde.HttpExchangeSerdeFactory;
import io.github.clescot.kafka.connect.http.serde.HttpResponseSerdeFactory;
import io.github.clescot.kafka.connect.http.serde.SerdeFactory;
import io.github.clescot.kafka.connect.http.sink.HttpSinkConnectorConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaJsonSerializerConfig.JSON_INDENT_OUTPUT;
import static io.confluent.kafka.serializers.KafkaJsonSerializerConfig.WRITE_DATES_AS_ISO8601;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES;

public class PublishConfigurer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublishConfigurer.class);
    public static final String PRODUCER_PREFIX = "producer.";
    public static final String JSON_SCHEMA = "json";
    public static final String BEARER_AUTH_SUB_CLAIM_NAME = "bearer.auth.sub.claim.name";
    public static final String BEARER_AUTH_SCOPE_CLAIM_NAME = "bearer.auth.scope.claim.name";
    public static final String BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS = "bearer.auth.cache.expiry.buffer.seconds";
    public static final String MISSING_CACHE_SIZE = "missing.cache.size";
    public static final String MISSING_SCHEMA_CACHE_TTL_SEC = "missing.schema.cache.ttl.sec";
    public static final String MISSING_VERSION_CACHE_TTL_SEC = "missing.version.cache.ttl.sec";
    public static final String MISSING_ID_CACHE_TTL_SEC = "missing.id.cache.ttl.sec";
    private static final List<String> JSON_SCHEMA_VERSIONS = Lists.newArrayList("draft_4", "draft_6", "draft_7", "draft_2019_09");

    //tests only
    private PublishConfigurer() {
    }

    public static PublishConfigurer build(){
        return new PublishConfigurer();
    }

    public void configureProducerPublishMode(HttpSinkConnectorConfig httpSinkConnectorConfig, KafkaProducer<String, Object> producer) {

        Preconditions.checkNotNull(httpSinkConnectorConfig,"'httpSinkConnectorConfig' is null but required");

        //low-level producer is configured (bootstrap.servers is a requirement)
        Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerBootstrapServers()), "producer.bootstrap.servers is not set.\n" + httpSinkConnectorConfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerSuccessTopic()), "producer.success.topic is not set.\n" + httpSinkConnectorConfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(httpSinkConnectorConfig.getProducerErrorTopic()), "producer.error.topic is not set.\n" + httpSinkConnectorConfig);
        Serializer<Object> serializer =  getSerializer(httpSinkConnectorConfig);
        Map<String, Object> producerSettings = httpSinkConnectorConfig.originalsWithPrefix(PRODUCER_PREFIX);
        producer.configure(producerSettings, new StringSerializer(), serializer);

        //connectivity check for producer
        checkKafkaConnectivity(httpSinkConnectorConfig, producer);
    }

    private void checkKafkaConnectivity(HttpSinkConnectorConfig sinkConnectorConfig, KafkaProducer<String, Object> producer) {
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

    public Queue<KafkaRecord> configureInMemoryQueue(HttpSinkConnectorConfig connectorConfig) {
        Preconditions.checkNotNull(connectorConfig,"connectorConfig is required but 'null'");
        String queueName = connectorConfig.getQueueName();
        Queue<KafkaRecord> queue = QueueFactory.getQueue(queueName);
        Preconditions.checkArgument(QueueFactory.hasAConsumer(
                queueName,
                connectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs()
                , connectorConfig.getPollDelayRegistrationOfQueueConsumerInMs(),
                connectorConfig.getPollIntervalRegistrationOfQueueConsumerInMs()
        ), "timeout : '" + connectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs() +
                "'ms timeout reached :" + queueName + "' queue hasn't got any consumer, " +
                "i.e no Source Connector has been configured to consume records published in this in memory queue. " +
                "we stop the Sink Connector to prevent any OutOfMemoryError.");
        return queue;
    }

    private <T> Serializer<T> getSerializer(HttpSinkConnectorConfig httpSinkConnectorConfig) {
        Serializer<T> serializer;
        String format = httpSinkConnectorConfig.getProducerFormat();
        LOGGER.info("producer format:'{}'", format);
        String content = httpSinkConnectorConfig.getProducerContent();
        LOGGER.info("producer content:'{}'", content);
        boolean writeDatesAsIso8601 = httpSinkConnectorConfig.isProducerJsonWriteDatesAs8601();
        //if format is json
        if (JSON_SCHEMA.equalsIgnoreCase(format)) {
            //json schema serde config
            Map<String, Object> serdeConfig = Maps.newHashMap();

            String schemaRegistryUrl = httpSinkConnectorConfig.getProducerSchemaRegistryUrl();
            serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

            boolean autoRegisterSchemas = httpSinkConnectorConfig.isProducerSchemaRegistryautoRegister();
            serdeConfig.put(AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'autoRegisterSchemas':'{}'", autoRegisterSchemas);

            String jsonSchemaSpecVersion = httpSinkConnectorConfig.isProducerJsonSchemaSpecVersion();
            Preconditions.checkNotNull(jsonSchemaSpecVersion);
            Preconditions.checkArgument(!jsonSchemaSpecVersion.isEmpty(), "'jsonSchemaSpecVersion' must not be an empty string");
            Preconditions.checkArgument(JSON_SCHEMA_VERSIONS.contains(jsonSchemaSpecVersion.toLowerCase()), "jsonSchemaSpecVersion supported values are 'draft_4','draft_6','draft_7','draft_2019_09' but not '" + jsonSchemaSpecVersion + "'");
            serdeConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecVersion);
            LOGGER.info("producer jsonSchemaSerdeConfigFactory: 'jsonSchemaSpecVersion':'{}'", jsonSchemaSpecVersion);


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

            SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient(httpSinkConnectorConfig, schemaRegistryUrl);
            SerdeFactory serdeFactory;
            if(content.equalsIgnoreCase("response")){
                serdeFactory = new HttpResponseSerdeFactory(schemaRegistryClient, serdeConfig);
            }else {
                serdeFactory = new HttpExchangeSerdeFactory(schemaRegistryClient, serdeConfig);
            }
            serializer = serdeFactory.buildSerde(false).serializer();
        } else {
            //serialize as a simple string
            serializer = new KafkaJsonSerializer<>();
            Map<String,Object> serializerConfig = Maps.newHashMap();
            boolean jsonIndentOutput = httpSinkConnectorConfig.getProducerJsonIndentOutput();
            serializerConfig.put(JSON_INDENT_OUTPUT,jsonIndentOutput);
            serializerConfig.put(WRITE_DATES_AS_ISO8601,writeDatesAsIso8601);
            serializer.configure(serializerConfig,false);
        }
        return serializer;
    }

    @NotNull
    private SchemaRegistryClient getSchemaRegistryClient(HttpSinkConnectorConfig httpSinkConnectorConfig, String schemaRegistryUrl) {

        Map<String, Object> schemaRegistryClientConfig = Maps.newHashMap();
        if (httpSinkConnectorConfig.getMissingIdCacheTTLSec() != null) {
            schemaRegistryClientConfig.put(MISSING_ID_CACHE_TTL_SEC, httpSinkConnectorConfig.getMissingIdCacheTTLSec());
        }
        if (httpSinkConnectorConfig.getMissingVersionCacheTTLSec() != null) {
            schemaRegistryClientConfig.put(MISSING_VERSION_CACHE_TTL_SEC, httpSinkConnectorConfig.getMissingVersionCacheTTLSec());
        }
        if (httpSinkConnectorConfig.getMissingSchemaCacheTTLSec() != null) {
            schemaRegistryClientConfig.put(MISSING_SCHEMA_CACHE_TTL_SEC, httpSinkConnectorConfig.getMissingVersionCacheTTLSec());
        }
        if (httpSinkConnectorConfig.getMissingCacheSize() != null) {
            schemaRegistryClientConfig.put(MISSING_CACHE_SIZE, httpSinkConnectorConfig.getMissingCacheSize());
        }
        if (httpSinkConnectorConfig.getMissingCacheSize() != null) {
            schemaRegistryClientConfig.put(BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS, httpSinkConnectorConfig.getBearerAuthCacheExpiryBufferSeconds());
        }
        if (httpSinkConnectorConfig.getBearerAuthScopeClaimName() != null) {
            schemaRegistryClientConfig.put(BEARER_AUTH_SCOPE_CLAIM_NAME, httpSinkConnectorConfig.getBearerAuthScopeClaimName());
        }
        if (httpSinkConnectorConfig.getBearerAuthSubClaimName() != null) {
            schemaRegistryClientConfig.put(BEARER_AUTH_SUB_CLAIM_NAME, httpSinkConnectorConfig.getBearerAuthSubClaimName());
        }

        RestService restService = new RestService(schemaRegistryUrl);
        int schemaRegistryCacheCapacity = httpSinkConnectorConfig.getProducerSchemaRegistryCacheCapacity();
        List<SchemaProvider> schemaProviders = Lists.newArrayList();
        schemaProviders.add(new JsonSchemaProvider());
        Map<String, String> httpHeaders = Maps.newHashMap();
        return new CachedSchemaRegistryClient(restService, schemaRegistryCacheCapacity, schemaProviders, schemaRegistryClientConfig, httpHeaders);
    }
}
