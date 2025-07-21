package io.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
import io.github.clescot.kafka.connect.http.source.cron.CronJobConfig;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.*;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

@Disabled
@Testcontainers
public class ITConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ITConnectorTest.class);
    private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams();
    public static final String CONFLUENT_VERSION = "8.0.0";
    public static final int CACHE_CAPACITY = 100;
    public static final String HTTP_REQUESTS_AS_STRING = "http-requests-string";
    public static final String PUBLISH_TO_IN_MEMORY_QUEUE_OK = PublishMode.IN_MEMORY_QUEUE.name();
    private static final Network NETWORK = Network.newNetwork();

    public static final String JKS_STORE_TYPE = "jks";
    public static final String TRUSTSTORE_PKIX_ALGORITHM = "PKIX";
    public static final String CLIENT_TRUSTSTORE_JKS_FILENAME = "client_truststore.jks";
    public static final String CLIENT_TRUSTSTORE_JKS_PASSWORD = "Secret123!";
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    @Container
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
            .withKraft()
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams().withPrefix("kafka-broker"))
            ;


    @Container
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION)
            .withNetwork(NETWORK)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams().withPrefix("schema-registry"))
            .dependsOn(kafkaContainer)
            .withStartupTimeout(Duration.ofSeconds(90));
    private static final String FAKE_SSL_DOMAIN_NAME = "it.mycorp.com";


    @Container
    public static DebeziumContainer connectContainer = new DebeziumContainer("clescot/kafka-connect-http:" + VERSION_UTILS.getVersion())
            .withFileSystemBind("target/http-connector", "/usr/local/share/kafka/plugins")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withNetwork(NETWORK)
            .withKafka(kafkaContainer)
            .withExtraHost(FAKE_SSL_DOMAIN_NAME, getIP())
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaContainer.getNetworkAliases().get(0) + ":9092")
            .withEnv("CONNECT_GROUP_ID", "test")
            .withEnv("CONNECT_MEMBER_ID", "test")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "test_config")
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "test_offset")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "test_status")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "pop-os.localdomain")
            .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "ERROR")
            .withEnv("CONNECT_LOG4J_LOGGERS", "" +
                    "org.glassfish=ERROR," +
                    "org.apache.kafka.connect=ERROR," +
                    "io.github.clescot=DEBUG," +
                    "org.apache.kafka.connect.runtime.distributed=ERROR," +
                    "org.apache.kafka.connect.runtime.isolation=ERROR," +
                    "org.reflections=ERROR," +
                    "org.apache.kafka.clients=ERROR")
//            .withEnv("KAFKA_DEBUG","true")
//            .withEnv("KAFKA_HEAP_OPTS","")
//            .withEnv("KAFKA_JVM_PERFORMANCE_OPTS","")
//            .withEnv("KAFKA_GC_LOG_OPTS","")
//            .withEnv("KAFKA_LOG4J_OPTS","")
//            .withEnv(" KAFKA_JMX_OPTS","")
//            .withEnv("DEFAULT_JAVA_DEBUG_OPTS","-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=$JAVA_DEBUG_PORT")
//            .withEnv("DEBUG_SUSPEND_FLAG","n")
//            .withEnv("JAVA_DEBUG_PORT","*:5005")
            .withCopyFileToContainer(MountableFile.forClasspathResource(CLIENT_TRUSTSTORE_JKS_FILENAME), "/opt/" + CLIENT_TRUSTSTORE_JKS_FILENAME)
            .withExposedPorts(8083)
//            .withExposedPorts(8083,5005)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams().withPrefix("kafka-connect"))
            .dependsOn(kafkaContainer, schemaRegistryContainer)
            .waitingFor(Wait.forHttp("/connector-plugins/"));


    private static String hostName;
    private static String internalSchemaRegistryUrl;
    private static String externalSchemaRegistryUrl;


    @RegisterExtension
    static WireMockExtension wmHttp = WireMockExtension.newInstance()
            .options(
                    WireMockConfiguration.wireMockConfig()
                            .dynamicPort()
            )
            .build();

    @RegisterExtension
    static WireMockExtension wmHttps = WireMockExtension.newInstance()
            .options(
                    WireMockConfiguration.wireMockConfig()
                            .keystorePath("src/test/resources/" + CLIENT_TRUSTSTORE_JKS_FILENAME)
                            .keystorePassword(CLIENT_TRUSTSTORE_JKS_PASSWORD)
                            .keystoreType("JKS")
                            .keyManagerPassword(CLIENT_TRUSTSTORE_JKS_PASSWORD)
                            .dynamicHttpsPort()
            )
            .build();

    @BeforeAll
    public static void startContainers() throws IOException {
        hostName = InetAddress.getLocalHost().getHostName();

        //start containers
        Startables.deepStart(Stream.of(kafkaContainer, schemaRegistryContainer, connectContainer)).join();
        internalSchemaRegistryUrl = "http://" + schemaRegistryContainer.getNetworkAliases().get(0) + ":8081";
        externalSchemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":" + schemaRegistryContainer.getMappedPort(8081);


        kafkaContainer.followOutput(logConsumer);
//        Integer mappedPort = connectContainer.getMappedPort(5005);
//        var config = new StaticTcpProxyConfig(
//                5005,
//                connectContainer.getHost(),
//                mappedPort
//        );
//        LOGGER.info("debug 5005 mapped port :'{}'",mappedPort);
//        config.setWorkerCount(1);
//        var tcpProxy = new TcpProxy(config);
//        tcpProxy.start();
    }

    private static void createTopics(String bootstrapServers, String... topics) {
        var newTopics =
                Arrays.stream(topics)
                        .map(topic -> new NewTopic(topic, 1, (short) 1))
                        .collect(Collectors.toList());
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            CreateTopicsResult createTopicsResult = admin.createTopics(newTopics);
            createTopicsResult.all().get();
            admin.listTopics().names().thenApply(
                    set-> set.stream().peek(topic-> LOGGER.info("topic:'{}' is present",topic))
            ).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void configureSinkConnector(String connectorName,
                                               String publishMode,
                                               String incomingTopic,
                                               String valueConverterClassName,
                                               String queueNameOrProducerTopic,
                                               Map.Entry<String, String>... additionalSettings) {
        ConnectorConfiguration sinkConnectorMessagesAsStringConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector")
                .with("tasks.max", "1")
                .with("topics", incomingTopic)
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", valueConverterClassName)
                .with("value.converter.use.optional.for.nonrequired", true)
                .with(PUBLISH_MODE, publishMode);
        if (PublishMode.IN_MEMORY_QUEUE.name().equalsIgnoreCase(publishMode)) {
            sinkConnectorMessagesAsStringConfiguration = sinkConnectorMessagesAsStringConfiguration.with("queue.name", queueNameOrProducerTopic);
        } else if (PublishMode.PRODUCER.name().equalsIgnoreCase(publishMode)) {
            sinkConnectorMessagesAsStringConfiguration = sinkConnectorMessagesAsStringConfiguration
                    .with("producer.bootstrap.servers", "kafka:9092")
                    .with("producer.schema.registry.url", internalSchemaRegistryUrl)
                    .with("producer.success.topic", queueNameOrProducerTopic)
                    .with("producer.error.topic", "http-error")
            ;
        }

        if (additionalSettings != null && additionalSettings.length > 0) {
            for (Map.Entry<String, String> additionalSetting : additionalSettings) {
                sinkConnectorMessagesAsStringConfiguration = sinkConnectorMessagesAsStringConfiguration.with(additionalSetting.getKey(), additionalSetting.getValue());
            }
        }
        connectContainer.registerConnector(connectorName, sinkConnectorMessagesAsStringConfiguration);
//        connectContainer.ensureConnectorTaskState(connectorName, 0, Connector.State.RUNNING);
    }

    private static void configureHttpSourceConnector(String connectorName, String queueName, String successTopic, String errorTopic) {
        //source connector
        ConnectorConfiguration sourceConnectorConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "io.github.clescot.kafka.connect.http.source.HttpSourceConnector")
                .with("tasks.max", "1")
                .with("success.topic", successTopic)
                .with("error.topic", errorTopic)
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "io.confluent.connect.json.JsonSchemaConverter")
                .with("value.converter.schema.registry.url", internalSchemaRegistryUrl)
                .with("queue.name", queueName);

        connectContainer.registerConnector(connectorName, sourceConnectorConfiguration);
        connectContainer.ensureConnectorTaskState(connectorName, 0, Connector.State.RUNNING);
    }
    private static void configureCronSourceConnector(String connectorName, String topic, List<CronJobConfig> configs) {
        //source connector
        String jobs = Joiner.on(",").join(configs.stream().map(CronJobConfig::getId).collect(Collectors.toList()));
        ConnectorConfiguration sourceConnectorConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector")
                .with("tasks.max", "1")
                .with("topic", topic)
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("jobs",jobs);

        for (CronJobConfig config : configs) {
            sourceConnectorConfiguration.with(config.getId()+".url",config.getUrl());
            sourceConnectorConfiguration.with(config.getId()+".cron",config.getCronExpression());
            sourceConnectorConfiguration.with(config.getId()+".method",config.getMethod().name());
            if(config.getBody()!=null) {
                sourceConnectorConfiguration.with(config.getId() + ".body", config.getBody());
            }
            Map<String, List<String>> headers = config.getHeaders();
            if(headers !=null && !headers.isEmpty()){
                Set<String> keySet = headers.keySet();
                String headerKeys = Joiner.on(",").join(new ArrayList<>(keySet));
                sourceConnectorConfiguration.with(config.getId() + ".headers",headerKeys);
                keySet.forEach(key-> sourceConnectorConfiguration.with(config.getId() + ".header."+key,Joiner.on(",").join(headers.get(key))));
            }
        }

        connectContainer.registerConnector(connectorName, sourceConnectorConfiguration);
        connectContainer.ensureConnectorTaskState(connectorName, 0, Connector.State.RUNNING);
    }


    @AfterEach
    public void afterEach() {
        wmHttp.resetAll();
        wmHttps.resetAll();
        connectContainer.deleteAllConnectors();
        QueueFactory.clearRegistrations();
    }

    @Test
    void test_sink_and_source_with_input_as_string() throws JSONException, JsonProcessingException {

        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String queueName = "test_sink_and_source_with_input_as_string";
        String successTopic = "success-test_sink_and_source_with_input_as_string";
        String errorTopic = "error-test_sink_and_source_with_input_as_string";
        checkConnectorPluginIsInstalled();
        configureHttpSourceConnector("http-source-connector-test_sink_and_source_with_input_as_string", queueName, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_sink_and_source_with_input_as_string",
                PublishMode.IN_MEMORY_QUEUE.name(),
                HTTP_REQUESTS_AS_STRING,
                "org.apache.kafka.connect.storage.StringConverter", "test_sink_and_source_with_input_as_string",
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true")
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        wireMock
                .register(WireMock.post("/ping")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(bodyResponse)
                                .withStatus(200)
                                .withStatusMessage("OK")
                        )
                );

        //forge messages which will command http requests
        KafkaProducer<String, String> producer = getStringProducer(kafkaContainer);

        String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                HttpRequest.Method.POST
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        String httpRequestAsJSON = MAPPER.writeValueAsString(httpRequest);
        ProducerRecord<String, String> record = new ProducerRecord<>(HTTP_REQUESTS_AS_STRING, null, System.currentTimeMillis(), null, httpRequestAsJSON, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String, HttpExchange> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,"json");

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, HttpExchange>> consumerRecords = drain(consumer, 1, 30);
        Assertions.assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, HttpExchange> consumerRecord = consumerRecords.get(0);
        Assertions.assertThat(consumerRecord.key()).isNull();
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"httpRequest\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \"" + baseUrl + "/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"httpResponse\": {" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \"OK\",\n" +
                "  \"responseHeaders\": {},\n" +
                "  \"responseBody\": \"" + escapedJsonResponse + "\"\n" +
                "}" +
                "}";

        String httpExchangeAsString = serializeHttpExchange(consumerRecord);
        JSONAssert.assertEquals(expectedJSON, httpExchangeAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        Assertions.assertThat(consumerRecord.headers().toArray()).isEmpty();
    }

    @NotNull
    private static String serializeHttpExchange(ConsumerRecord<String, HttpExchange> consumerRecord) {
        Serializer jsonStringSerializer = new KafkaJsonSerializer();
        return new String(jsonStringSerializer.serialize("dummy", consumerRecord.value()), StandardCharsets.UTF_8);
    }


    @Test
    void test_sink_in_producer_mode_with_input_as_string_and_producer_output_as_string() throws JSONException, JsonProcessingException {

        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        String incomingTopic = "http_requests_string_for_producer_output_as_string";

        String successTopic = "success_test_sink_and_source_with_input_as_string_and_producer_output_as_string";
        String errorTopic = "error_test_sink_and_source_with_input_as_string_and_producer_output_as_string";

        //register connectors
        checkConnectorPluginIsInstalled();

        String producerOutputFormat = "string";
        configureSinkConnector("http-sink-connector-test_sink_and_source_with_input_as_string",
                PublishMode.PRODUCER.name(),
                incomingTopic,
                "org.apache.kafka.connect.storage.StringConverter", successTopic,
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>("producer.format", producerOutputFormat)
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        wireMock
                .register(WireMock.post("/ping")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(bodyResponse)
                                .withStatus(200)
                                .withStatusMessage("OK")
                        )
                );
        //forge messages which will command http requests
        KafkaProducer<String, String> producer = getStringProducer(kafkaContainer);

        String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                HttpRequest.Method.POST
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        String httpRequestAsJSON = MAPPER.writeValueAsString(httpRequest);
        ProducerRecord<String, String> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequestAsJSON, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String, String> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,producerOutputFormat);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, String>> consumerRecords = drain(consumer, 1, 120);
        Assertions.assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, String> consumerRecord = consumerRecords.get(0);
        Assertions.assertThat(consumerRecord.key()).isNull();
        String httpExchangeAsString = consumerRecord.value();
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"httpRequest\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \"" + baseUrl + "/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"httpResponse\": {" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \"OK\",\n" +
                "  \"responseHeaders\": {},\n" +
                "  \"responseBody\": \"" + escapedJsonResponse + "\"\n" +
                "}" +
                "}";
        JSONAssert.assertEquals(expectedJSON, httpExchangeAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        Assertions.assertThat(consumerRecord.headers().toArray()).isEmpty();
    }

    @Test
    void test_sink_in_producer_mode_with_input_as_string_and_producer_output_as_string_with_cron() throws JSONException, JsonProcessingException {

        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        String incomingTopic = "http_requests_string_for_producer_output_as_string";

        String successTopic = "success_test_sink_and_source_with_input_as_string_and_producer_output_as_string";
        String errorTopic = "error_test_sink_and_source_with_input_as_string_and_producer_output_as_string";

        //register connectors
        checkConnectorPluginIsInstalled();

        String producerOutputFormat = "string";
        configureSinkConnector("http-sink-connector-test_sink_and_source_with_input_as_string",
                PublishMode.PRODUCER.name(),
                incomingTopic,
                "org.apache.kafka.connect.storage.StringConverter", successTopic,
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>("producer.format", producerOutputFormat)
        );

        String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        headers.put("Accept", Lists.newArrayList("text/html","application/xhtml+xml","application/xml;q=0.9","*/*;q=0.8"));

        CronJobConfig cronJobConfig = new CronJobConfig("job1",url,"0/5 * * ? * *",HttpRequest.Method.POST,"stuff",headers);
        configureCronSourceConnector("cron-source-connector",incomingTopic,Lists.newArrayList(cronJobConfig));
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        wireMock
                .register(WireMock.post("/ping")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(bodyResponse)
                                .withStatus(200)
                                .withStatusMessage("OK")
                        )
                );



        //verify http responses
        KafkaConsumer<String, String> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,producerOutputFormat);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, String>> consumerRecords = drain(consumer, 1, 120);
        Assertions.assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, String> consumerRecord = consumerRecords.get(0);
        Assertions.assertThat(consumerRecord.key()).isNull();
        String httpExchangeAsString = consumerRecord.value();
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"httpRequest\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \"" + baseUrl + "/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"httpResponse\": {" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \"OK\",\n" +
                "  \"responseHeaders\": {},\n" +
                "  \"responseBody\": \"" + escapedJsonResponse + "\"\n" +
                "}" +
                "}";
        JSONAssert.assertEquals(expectedJSON, httpExchangeAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        Assertions.assertThat(consumerRecord.headers().toArray()).isEmpty();
    }




    @Test
    void test_sink_in_producer_mode_with_input_as_string_and_producer_output_as_json() throws JSONException, JsonProcessingException {

        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        String incomingTopic = "http_requests_string_for_producer_output_as_json";

        String successTopic = "success_test_sink_and_source_with_input_as_string_and_producer_output_as_json";
        String errorTopic = "error_test_sink_and_source_with_input_as_string_and_producer_output_as_json";

        //register connectors
        checkConnectorPluginIsInstalled();

        String producerOutputFormat = "json";
        configureSinkConnector("http-sink-connector-test_sink_and_source_with_input_as_string",
                PublishMode.PRODUCER.name(),
                incomingTopic,
                "org.apache.kafka.connect.storage.StringConverter", successTopic,
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>("producer.format", producerOutputFormat),
                new AbstractMap.SimpleImmutableEntry<>("producer.key.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"),
                new AbstractMap.SimpleImmutableEntry<>("producer.value.subject.name.strategy", "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy")
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        wireMock
                .register(WireMock.post("/ping")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(bodyResponse)
                                .withStatus(200)
                                .withStatusMessage("OK")
                        )
                );
        //forge messages which will command http requests
        KafkaProducer<String, String> producer = getStringProducer(kafkaContainer);

        String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                HttpRequest.Method.POST
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        String httpRequestAsJSON = MAPPER.writeValueAsString(httpRequest);
        ProducerRecord<String, String> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequestAsJSON, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String, HttpExchange> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,producerOutputFormat);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, HttpExchange>> consumerRecords = drain(consumer, 1, 180);
        Assertions.assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, HttpExchange> consumerRecord = consumerRecords.get(0);
        Assertions.assertThat(consumerRecord.key()).isNull();
        HttpExchange httpExchange = consumerRecord.value();
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"httpRequest\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \"" + baseUrl + "/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"httpResponse\": {" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \"OK\",\n" +
                "  \"responseHeaders\": {},\n" +
                "  \"responseBody\": \"" + escapedJsonResponse + "\"\n" +
                "}" +
                "}";
        Serializer jsonStringSerializer = new KafkaJsonSerializer();
        String httpExchangeAsString = new String(jsonStringSerializer.serialize("dummy", httpExchange), StandardCharsets.UTF_8);
        JSONAssert.assertEquals(expectedJSON, httpExchangeAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        Assertions.assertThat(consumerRecord.headers().toArray()).isEmpty();
    }

    private static void checkConnectorPluginIsInstalled() {
        OkHttpClient okHttpClient = new OkHttpClient.Builder().build();
        Request.Builder builder = new Request.Builder();
        Request request = builder.url(connectContainer.getTarget() + "/connector-plugins")
                .build();
        Awaitility.await().pollInterval(Duration.of(5, ChronoUnit.SECONDS)).atMost(Duration.of(2, ChronoUnit.MINUTES)).until(() -> {
            Response response = okHttpClient.newCall(request).execute();
            String content = response.body().string();
            System.out.println(content);
            return content.contains("HttpSinkConnector")
                    && content.contains("HttpSourceConnector")
                    && content.contains("CronSourceConnector");
        });
    }

    @Test
    void test_sink_and_source_with_input_as_struct_and_schema_registry() throws JSONException {
        WireMockRuntimeInfo httpRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String suffix = "sink_and_source_with_input_as_struct_and_schema_registry";
        String incomingTopic = "incoming-" + suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        configureHttpSourceConnector("http-source-connector-test_" + suffix, "test_" + suffix, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url", internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true")
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = httpRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        System.out.println(bodyResponse);
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        System.out.println(escapedJsonResponse);
        int statusCode = 200;
        String statusMessage = "OK";
        wireMock
                .register(WireMock.post("/ping")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(bodyResponse)
                                .withStatus(statusCode)
                                .withStatusMessage(statusMessage)
                        )
                );

        //forge messages which will command http requests
        KafkaProducer<String, HttpRequest> producer = getStructAsJSONProducer();

        String baseUrl = "http://" + getIP() + ":" + httpRuntimeInfo.getHttpPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                HttpRequest.Method.POST
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        ProducerRecord<String, HttpRequest> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequest, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String, HttpExchange> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,"json");

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, HttpExchange>> consumerRecords = drain(consumer, 1, 120);
        Assertions.assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, HttpExchange> consumerRecord = consumerRecords.get(0);
        Assertions.assertThat(consumerRecord.topic()).isEqualTo(successTopic);
        Assertions.assertThat(consumerRecord.key()).isNull();
        String jsonAsString = serializeHttpExchange(consumerRecord);
        LOGGER.info("json response  :{}", jsonAsString);
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"httpRequest\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \"" + baseUrl + "/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"httpResponse\": {\n" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \"" + statusMessage + "\",\n" +
                "  \"responseHeaders\": {" +
                "\"Content-Type\":[\"application/json\"]" +
                "},\n" +
                "  \"responseBody\": \"" + escapedJsonResponse + "\"\n" +
                "}" +
                "}";
        JSONAssert.assertEquals(expectedJSON, jsonAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        Assertions.assertThat(consumerRecord.headers().toArray()).isEmpty();

    }

    @Test
    void test_retry_policy() throws JSONException {
        WireMockRuntimeInfo httpRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String suffix = "retry_policy";
        String incomingTopic = "incoming-" + suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        configureHttpSourceConnector("http-source-connector-test_" + suffix, "test_" + suffix, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url", internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RETRIES, "3"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RETRY_DELAY_IN_MS, "1000"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS, "100000"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RETRY_DELAY_FACTOR, "1.5"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RETRY_JITTER_IN_MS, "500"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX, DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX)
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = httpRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        System.out.println(bodyResponse);
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        System.out.println(escapedJsonResponse);
        int successStatusCode = 200;
        String successStatusMessage = "OK";
        int serverErrorStatusCode = 500;
        String errorStatusMessage = "Internal Server Error";
        wireMock
                .register(WireMock.post(WireMock.urlEqualTo("/ping"))
                        .inScenario("retry-policy")
                        .whenScenarioStateIs(Scenario.STARTED)
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withHeader("X-Try", "1")
                                .withBody(bodyResponse)
                                .withStatus(serverErrorStatusCode)
                                .withStatusMessage(errorStatusMessage)
                        )
                        .willSetStateTo("2nd step")
                );
        wireMock
                .register(WireMock.post(WireMock.urlEqualTo("/ping"))
                        .inScenario("retry-policy")
                        .whenScenarioStateIs("2nd step")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withHeader("X-Try", "2")
                                .withBody(bodyResponse)
                                .withStatus(serverErrorStatusCode)
                                .withStatusMessage(errorStatusMessage)
                        )
                        .willSetStateTo("3rd step")
                );
        wireMock
                .register(WireMock.post(WireMock.urlEqualTo("/ping"))
                        .inScenario("retry-policy")
                        .whenScenarioStateIs("3rd step")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withHeader("X-Try", "3")
                                .withBody(bodyResponse)
                                .withStatus(successStatusCode)
                                .withStatusMessage(successStatusMessage)
                        )
                );
        //forge messages which will command http requests
        KafkaProducer<String, HttpRequest> producer = getStructAsJSONProducer();

        String baseUrl = "http://" + getIP() + ":" + httpRuntimeInfo.getHttpPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                HttpRequest.Method.POST
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        ProducerRecord<String, HttpRequest> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequest, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String, HttpExchange> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,"json");

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        LOGGER.info("subscribing to successTopic:'{}' and errorTopic:'{}'",successTopic,errorTopic);

        int messageCount = 1;
        List<ConsumerRecord<String, HttpExchange>> consumerRecords = drain(consumer, messageCount, 120);
        Assertions.assertThat(consumerRecords).hasSize(1);
        int messageInErrorTopic = 0;
        int messageInSuccessTopic = 0;
        for (int i = 0; i < messageCount; i++) {
            ConsumerRecord<String, HttpExchange> consumerRecord = consumerRecords.get(i);
            if (errorTopic.equals(consumerRecord.topic())) {
                messageInErrorTopic++;
                checkMessage(errorTopic, escapedJsonResponse, serverErrorStatusCode, errorStatusMessage, baseUrl, consumerRecord,1);
            } else {
                messageInSuccessTopic++;
                checkMessage(successTopic, escapedJsonResponse, successStatusCode, successStatusMessage, baseUrl, consumerRecord,3);
            }
        }
        Assertions.assertThat(messageInErrorTopic).isZero();
        Assertions.assertThat(messageInSuccessTopic).isEqualTo(1);
    }

    @Test
    void test_sink_and_source_with_input_as_struct_and_schema_registry_test_rate_limiting() throws JSONException {
        WireMockRuntimeInfo httpRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String suffix = "sink_and_source_with_input_as_struct_and_schema_registry_test_rate_limiting";
        String incomingTopic = "incoming-" + suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        String queueName = "test_" + suffix;
        String connectorName = "http-source-connector-test_" + suffix;
        configureHttpSourceConnector(connectorName, queueName, successTopic, errorTopic);
        int maxExecutionsPerSecond = 3;
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url", internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS, "1000"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, maxExecutionsPerSecond + "")
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = httpRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        System.out.println(bodyResponse);
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        System.out.println(escapedJsonResponse);
        int statusCode = 200;
        String statusMessage = "OK";
        wireMock
                .register(WireMock.post("/ping")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(bodyResponse)
                                .withStatus(statusCode)
                                .withStatusMessage(statusMessage)
                        )
                );

        //forge messages which will command http requests
        KafkaProducer<String, HttpRequest> producer = getStructAsJSONProducer();

        String baseUrl = "http://" + getIP() + ":" + httpRuntimeInfo.getHttpPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                HttpRequest.Method.POST
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        ProducerRecord<String, HttpRequest> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequest, kafkaHeaders);
        int messageCount = 50;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < messageCount; i++) {
            producer.send(record);
        }
        producer.flush();

        //verify http responses
        KafkaConsumer<String, HttpExchange> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,"json");

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, HttpExchange>> consumerRecords = drain(consumer, messageCount, 40);
        Assertions.assertThat(consumerRecords).hasSize(messageCount);

        int checkedMessages = 0;
        for (int i = 0; i < messageCount; i++) {
            ConsumerRecord<String, HttpExchange> consumerRecord = consumerRecords.get(i);
            checkMessage(successTopic, escapedJsonResponse, 200, statusMessage, baseUrl, consumerRecord,1);
            checkedMessages++;
        }
        stopwatch.stop();

        int minExecutionTimeInSeconds = messageCount / (maxExecutionsPerSecond);
        LOGGER.info("min execution time  '{}' seconds", minExecutionTimeInSeconds);
        long elapsedTimeInSeconds = stopwatch.elapsed(SECONDS);
        LOGGER.info("elapsed time '{}' seconds", elapsedTimeInSeconds);
        //we add one to avoid rounding issues
        Assertions.assertThat(elapsedTimeInSeconds + 1).isGreaterThan(minExecutionTimeInSeconds);

    }

    @Test
    void test_custom_truststore() throws JSONException {


        WireMockRuntimeInfo httpsRuntimeInfo = wmHttps.getRuntimeInfo();
        //register connectors
        String suffix = "custom_truststore";
        String incomingTopic = "incoming-" + suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        String queueName = "test_" + suffix;
        configureHttpSourceConnector("http-source-connector-test_" + suffix, queueName, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url", internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_REQUEST_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_GENERATE_MISSING_CORRELATION_ID, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION, "true"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_DEFAULT_OKHTTP_PROTOCOLS, "HTTP_1_1"),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH, "/opt/" + CLIENT_TRUSTSTORE_JKS_FILENAME),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, CLIENT_TRUSTSTORE_JKS_PASSWORD),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE, JKS_STORE_TYPE),
                new AbstractMap.SimpleImmutableEntry<>(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM, TRUSTSTORE_PKIX_ALGORITHM)
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = httpsRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        System.out.println(bodyResponse);
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        System.out.println(escapedJsonResponse);
        int statusCode = 200;
        String statusMessage = "OK";
        wireMock
                .register(WireMock.post("/ping")
                        .willReturn(WireMock.aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withBody(bodyResponse)
                                .withStatus(statusCode)
                                .withStatusMessage(statusMessage)
                        )
                );

        //forge messages which will command http requests
        KafkaProducer<String, HttpRequest> producer = getStructAsJSONProducer();

        String baseUrl = "https://" + FAKE_SSL_DOMAIN_NAME + ":" + httpsRuntimeInfo.getHttpsPort();
        String url = baseUrl + "/ping";
        LOGGER.info("url:{}", url);
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                HttpRequest.Method.POST
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        ProducerRecord<String, HttpRequest> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequest, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String, HttpExchange> consumer = getConsumer(kafkaContainer, externalSchemaRegistryUrl,"json");

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, HttpExchange>> consumerRecords = drain(consumer, 1, 120);
        Assertions.assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, HttpExchange> consumerRecord = consumerRecords.get(0);
        Assertions.assertThat(consumerRecord.topic()).isEqualTo(successTopic);
        Assertions.assertThat(consumerRecord.key()).isNull();
        String jsonAsString = serializeHttpExchange(consumerRecord);
        LOGGER.info("json response  :{}", jsonAsString);
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"httpRequest\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \"" + baseUrl + "/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"httpResponse\": {\n" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \"" + statusMessage + "\",\n" +
                "  \"responseHeaders\": {" +
                "\"Content-Type\":[\"application/json\"]" +
                "},\n" +
                "  \"responseBody\": \"" + escapedJsonResponse + "\"\n" +
                "}" +
                "}";
        JSONAssert.assertEquals(expectedJSON, jsonAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        Assertions.assertThat(consumerRecord.headers().toArray()).isEmpty();
    }

    private static void checkMessage(String topicName, String escapedJsonResponse, int statusCode, String statusMessage, String baseUrl, ConsumerRecord<String, HttpExchange> consumerRecord,int attempts) throws JSONException {
        Assertions.assertThat(consumerRecord.topic()).isEqualTo(topicName);
        Assertions.assertThat(consumerRecord.key()).isNull();
        String jsonAsString = serializeHttpExchange(consumerRecord);
        LOGGER.info("topic:'{}' json response  :{}",topicName, jsonAsString);
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": "+attempts+",\n" +
                "  \"httpRequest\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \"" + baseUrl + "/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"httpResponse\": {\n" +
                "   \"statusCode\":" + statusCode + ",\n" +
                "  \"statusMessage\": \"" + statusMessage + "\",\n" +
                "  \"responseHeaders\": {" +
                "\"Content-Type\":[\"application/json\"]" +
                "},\n" +
                "  \"responseBody\": \"" + escapedJsonResponse + "\"\n" +
                "}" +
                "}";
        JSONAssert.assertEquals(expectedJSON, jsonAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Try", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        Assertions.assertThat(consumerRecord.headers().toArray()).isEmpty();
    }


    private KafkaProducer<String, String> getStringProducer(
            KafkaContainer kafkaContainer) {

        return new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaContainer.getBootstrapServers()
                ),
                new StringSerializer(),
                new StringSerializer());
    }

    private <T> KafkaProducer<String, T> getStructAsJSONProducer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, externalSchemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "true");
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, SpecificationVersion.DRAFT_2019_09.toString());
        props.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "false");
        props.put(FAIL_UNKNOWN_PROPERTIES, "true");
        props.put(FAIL_INVALID_SCHEMA, "true");
        props.put(WRITE_DATES_AS_ISO8601, "true");
        return new KafkaProducer<>(props);
    }

    private <T> KafkaConsumer<String, T> getConsumer(
            KafkaContainer kafkaContainer,
            String schemaRegistryUrl,String format) {

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                schemaRegistryUrl,
                CACHE_CAPACITY,
                Lists.newArrayList(
                        new JsonSchemaProvider(),
                        new AvroSchemaProvider()),
                Maps.newHashMap());
        Deserializer deserializer;
        switch(format) {
            case "json":
                HashMap<String, Object> props = Maps.newHashMap();
                props.put("schema.registry.url",schemaRegistryUrl);
                props.put(JSON_KEY_TYPE,String.class);
                props.put(JSON_VALUE_TYPE,HttpExchange.class);
                props.put(FAIL_INVALID_SCHEMA,true);
                props.put(FAIL_UNKNOWN_PROPERTIES,true);
                props.put(WRITE_DATES_AS_ISO8601,true);
                props.put(SCHEMA_SPEC_VERSION,"draft_2019_09");
                deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, props, HttpExchange.class);
                break;
            case "string":
            default:
                deserializer = Serdes.String().deserializer();
                break;
        }


        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "test-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"),
                new StringDeserializer(),
                deserializer);
    }

    private <T> List<ConsumerRecord<String, T>> drain(KafkaConsumer<String, T> consumer,
                                                                 int expectedRecordCount, int timeoutInSeconds) {
        List<ConsumerRecord<String, T>> allRecords = new ArrayList<>();
        Unreliables.retryUntilTrue(timeoutInSeconds, SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(record -> {
                        LOGGER.info("record received :{}", record);
                        allRecords.add(record);
                    });
            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }


    private static String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
