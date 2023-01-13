package com.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static com.github.clescot.kafka.connect.http.sink.HttpSinkTaskTest.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ITConnectorTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(ITConnectorTest.class);
    private final static Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams();
    public static final String CONFLUENT_VERSION = "7.3.0";
    public static final int CUSTOM_AVAILABLE_PORT = 0;
    public static final int CACHE_CAPACITY = 100;
    public static final String HTTP_REQUESTS_AS_STRING = "http-requests-string";
    public static final boolean PUBLISH_TO_IN_MEMORY_QUEUE_OK = true;
    private static Network network = Network.newNetwork();
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    @Container
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
            .withNetwork(network)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            ;
    @Container
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer)
            .withStartupTimeout(Duration.ofSeconds(90));
    private static final String FAKE_SSL_DOMAIN_NAME = "it.mycorp.com";
    @Container
    public static DebeziumContainer connectContainer = new DebeziumContainer("confluentinc/cp-kafka-connect:"+CONFLUENT_VERSION)
            .withFileSystemBind("target/http-connector", "/usr/local/share/kafka/plugins")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withExtraHost(FAKE_SSL_DOMAIN_NAME,getIP())
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaContainer.getNetworkAliases().get(0) + ":9092")
            .withEnv("CONNECT_GROUP_ID", "test")
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
                    "org.apache.kafka.connect=ERROR," +
                    "com.github.clescot=DEBUG," +
                    "org.apache.kafka.connect.runtime.distributed=ERROR," +
                    "org.apache.kafka.connect.runtime.isolation=ERROR," +
                    "org.reflections=ERROR," +
                    "org.apache.kafka.clients=ERROR")
            .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java/,/usr/share/confluent-hub-components/,/usr/local/share/kafka/plugins")
            .withCopyFileToContainer(MountableFile.forClasspathResource(CLIENT_TRUSTSTORE_JKS_FILENAME),"/opt/"+CLIENT_TRUSTSTORE_JKS_FILENAME)
            .withExposedPorts(8083)
            .dependsOn(kafkaContainer, schemaRegistryContainer)
            .waitingFor(Wait.forHttp("/connector-plugins/"));

    private static String hostName;
    private static String internalSchemaRegistryUrl;
    private static String externalSchemaRegistryUrl;


    @RegisterExtension
    static WireMockExtension wmHttp = WireMockExtension.newInstance()
            .options(
                    wireMockConfig()
                    .dynamicPort()
            )
            .build();

    @RegisterExtension
    static WireMockExtension wmHttps = WireMockExtension.newInstance()
            .options(
                    wireMockConfig()
                    .keystorePath("src/test/resources/"+CLIENT_TRUSTSTORE_JKS_FILENAME)
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
        externalSchemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":"+schemaRegistryContainer.getMappedPort(8081);
    }

    private static void configureSinkConnector(String connectorName, boolean publishToInMemoryQueue, String incomingTopic, String valueConverterClassName, String queueName, Map.Entry<String,String>... additionalSettings) {
        ConnectorConfiguration sinkConnectorMessagesAsStringConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.sink.HttpSinkConnector")
                .with("tasks.max", "1")
                .with("topics", incomingTopic)
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", valueConverterClassName)
                .with("value.converter.use.optional.for.nonrequired", true)
                .with(PUBLISH_TO_IN_MEMORY_QUEUE, Boolean.valueOf(publishToInMemoryQueue).toString());
        if(publishToInMemoryQueue){
            sinkConnectorMessagesAsStringConfiguration =sinkConnectorMessagesAsStringConfiguration.with("queue.name", queueName);
        }
        if(additionalSettings!=null && additionalSettings.length>0) {
            for (Map.Entry<String, String> additionalSetting : additionalSettings) {
                sinkConnectorMessagesAsStringConfiguration = sinkConnectorMessagesAsStringConfiguration.with(additionalSetting.getKey(), additionalSetting.getValue());
            }
        }
        connectContainer.registerConnector(connectorName, sinkConnectorMessagesAsStringConfiguration);
        connectContainer.ensureConnectorTaskState(connectorName, 0, Connector.State.RUNNING);
    }

    private static void configureSourceConnector(String connectorName, String queueName, String successTopic, String errorTopic) {
        //source connector
        ConnectorConfiguration sourceConnectorConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.source.HttpSourceConnector")
                .with("tasks.max", "1")
                .with("success.topic", successTopic)
                .with("error.topic", errorTopic)
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "io.confluent.connect.json.JsonSchemaConverter")
                .with("value.converter.schema.registry.url", internalSchemaRegistryUrl)
                .with("queue.name", queueName)
                ;

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
    public void test_sink_and_source_with_input_as_string() throws JSONException, JsonProcessingException {
        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String queueName = "test_sink_and_source_with_input_as_string";
        String successTopic = "success-test_sink_and_source_with_input_as_string";
        String errorTopic = "error-test_sink_and_source_with_input_as_string";
        configureSourceConnector("http-source-connector-test_sink_and_source_with_input_as_string", queueName, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_sink_and_source_with_input_as_string",
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                HTTP_REQUESTS_AS_STRING,
                "org.apache.kafka.connect.storage.StringConverter", "test_sink_and_source_with_input_as_string",
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_REQUEST_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_CORRELATION_ID,"true")
        );
        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();
        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);

        //define the http Mock Server interaction
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        String bodyResponse = "{\"result\":\"pong\"}";
        String escapedJsonResponse = StringEscapeUtils.escapeJson(bodyResponse);
        wireMock
                .register(post("/ping")
                        .willReturn(aResponse()
                        .withHeader("Content-Type","application/json")
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
        headers.put("X-Correlation-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                "POST",
                "STRING"
                );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        String httpRequestAsJSON = MAPPER.writeValueAsString(httpRequest);
        ProducerRecord<String, String> record = new ProducerRecord<>(HTTP_REQUESTS_AS_STRING, null, System.currentTimeMillis(), null, httpRequestAsJSON, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String,? extends Object> consumer = getConsumer(kafkaContainer,externalSchemaRegistryUrl);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, ? extends Object>> consumerRecords = drain(consumer, 1, 30);
        assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, ? extends Object> consumerRecord = consumerRecords.get(0);
        assertThat(consumerRecord.key()).isNull();
        String jsonAsString = consumerRecord.value().toString();
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"request\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \""+baseUrl+"/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"response\": {" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \"OK\",\n" +
                "  \"headers\": {},\n" +
                "  \"body\": \""+escapedJsonResponse+"\"\n" +
                "}"+
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
        assertThat(consumerRecord.headers().toArray()).isEmpty();
    }

    @Test
    public void test_sink_and_source_with_input_as_struct_and_schema_registry() throws JSONException {
        WireMockRuntimeInfo httpRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String suffix = "sink_and_source_with_input_as_struct_and_schema_registry";
        String incomingTopic="incoming-"+suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        configureSourceConnector("http-source-connector-test_" + suffix, "test_" + suffix, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url",internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_REQUEST_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_CORRELATION_ID,"true")
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
                .register(post("/ping")
                        .willReturn(aResponse()
                                .withHeader("Content-Type","application/json")
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
        headers.put("X-Correlation-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                "POST",
                "STRING"
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        ProducerRecord<String, HttpRequest> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequest, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String,? extends Object> consumer = getConsumer(kafkaContainer,externalSchemaRegistryUrl);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, ? extends Object>> consumerRecords = drain(consumer, 1, 120);
        assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, ? extends Object> consumerRecord = consumerRecords.get(0);
        assertThat(consumerRecord.topic()).isEqualTo(successTopic);
        assertThat(consumerRecord.key()).isNull();
        String jsonAsString = consumerRecord.value().toString();
        LOGGER.info("json response  :{}",jsonAsString);
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"request\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \""+baseUrl+"/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"response\": {\n" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \""+statusMessage+"\",\n" +
                "  \"headers\": {" +
                "\"Content-Type\":[\"application/json\"]" +
                "},\n" +
                "  \"body\": \""+escapedJsonResponse+"\"\n" +
                "}"+
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
        assertThat(consumerRecord.headers().toArray()).isEmpty();

    }

    @Test
    public void test_retry_policy() throws JSONException {
        WireMockRuntimeInfo httpRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String suffix = "retry_policy";
        String incomingTopic="incoming-"+suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        configureSourceConnector("http-source-connector-test_" + suffix, "test_" + suffix, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url",internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_REQUEST_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_CORRELATION_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_DEFAULT_RETRIES,"3"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_DEFAULT_RETRY_DELAY_IN_MS,"1000"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_DEFAULT_RETRY_MAX_DELAY_IN_MS,"100000"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_DEFAULT_RETRY_DELAY_FACTOR,"1.5"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_DEFAULT_RETRY_JITTER_IN_MS,"500")
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
                .register(post(urlEqualTo("/ping"))
                        .inScenario("retry-policy")
                        .whenScenarioStateIs(STARTED)
                        .willReturn(aResponse()
                                .withHeader("Content-Type","application/json")
                                .withHeader("X-Try","1")
                                .withBody(bodyResponse)
                                .withStatus(serverErrorStatusCode)
                                .withStatusMessage(errorStatusMessage)
                        )
                        .willSetStateTo("2nd step")
                );
        wireMock
                .register(post(urlEqualTo("/ping"))
                        .inScenario("retry-policy")
                        .whenScenarioStateIs("2nd step")
                        .willReturn(aResponse()
                                .withHeader("Content-Type","application/json")
                                .withHeader("X-Try","2")
                                .withBody(bodyResponse)
                                .withStatus(serverErrorStatusCode)
                                .withStatusMessage(errorStatusMessage)
                        )
                        .willSetStateTo("3rd step")
                );
        wireMock
                .register(post(urlEqualTo("/ping"))
                        .inScenario("retry-policy")
                        .whenScenarioStateIs("3rd step")
                        .willReturn(aResponse()
                                .withHeader("Content-Type","application/json")
                                .withHeader("X-Try","3")
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
        headers.put("X-Correlation-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                "POST",
                "STRING"
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        ProducerRecord<String, HttpRequest> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequest, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String,? extends Object> consumer = getConsumer(kafkaContainer,externalSchemaRegistryUrl);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        int messageCount = 3;
        List<ConsumerRecord<String, ? extends Object>> consumerRecords = drain(consumer, messageCount, 120);
        assertThat(consumerRecords).hasSize(3);
        int messageInErrorTopic=0;
        int messageInSuccessTopic=0;
        for (int i = 0; i < messageCount; i++) {
            ConsumerRecord<String, ? extends Object> consumerRecord = consumerRecords.get(i);
            if(errorTopic.equals(consumerRecord.topic())) {
                messageInErrorTopic++;
                checkMessage(errorTopic, escapedJsonResponse, serverErrorStatusCode, errorStatusMessage, baseUrl, consumerRecord);
            }else{
                messageInSuccessTopic++;
                checkMessage(successTopic, escapedJsonResponse, successStatusCode, successStatusMessage, baseUrl, consumerRecord);
            }
        }
        assertThat(messageInErrorTopic).isEqualTo(2);
        assertThat(messageInSuccessTopic).isEqualTo(1);
    }

    @Test
    public void test_sink_and_source_with_input_as_struct_and_schema_registry_test_rate_limiting() throws JSONException {
        WireMockRuntimeInfo httpRuntimeInfo = wmHttp.getRuntimeInfo();
        //register connectors
        String suffix = "sink_and_source_with_input_as_struct_and_schema_registry_test_rate_limiting";
        String incomingTopic= "incoming-" + suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        String queueName = "test_" + suffix;
        String connectorName = "http-source-connector-test_" + suffix;
        configureSourceConnector(connectorName, queueName, successTopic, errorTopic);
        int maxExecutionsPerSecond = 3;
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url",internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_REQUEST_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_CORRELATION_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_DEFAULT_RATE_LIMITER_PERIOD_IN_MS,"1000"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, maxExecutionsPerSecond+"")
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
                .register(post("/ping")
                        .willReturn(aResponse()
                                .withHeader("Content-Type","application/json")
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
        headers.put("X-Correlation-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                "POST",
                "STRING"
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
        KafkaConsumer<String,? extends Object> consumer = getConsumer(kafkaContainer,externalSchemaRegistryUrl);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, ? extends Object>> consumerRecords = drain(consumer, messageCount, 40);
        assertThat(consumerRecords).hasSize(messageCount);

        int checkedMessages=0;
        for (int i = 0; i < messageCount; i++) {
            ConsumerRecord<String, ? extends Object> consumerRecord = consumerRecords.get(i);
            checkMessage(successTopic,escapedJsonResponse, 200, statusMessage, baseUrl, consumerRecord);
            checkedMessages++;
        }
        stopwatch.stop();

        int minExecutionTimeInSeconds = messageCount / (maxExecutionsPerSecond);
        LOGGER.info("min execution time  '{}' seconds",minExecutionTimeInSeconds);
        long elapsedTimeInSeconds = stopwatch.elapsed(SECONDS);
        LOGGER.info("elapsed time '{}' seconds",elapsedTimeInSeconds);
        //we add one to avoid rounding issues
        assertThat(elapsedTimeInSeconds+1).isGreaterThan(minExecutionTimeInSeconds);

    }
    @Test
    public void test_custom_truststore() throws JSONException {


        WireMockRuntimeInfo httpsRuntimeInfo = wmHttps.getRuntimeInfo();
        //register connectors
        String suffix = "custom_truststore";
        String incomingTopic = "incoming-"+suffix;
        String successTopic = "success-" + suffix;
        String errorTopic = "error-" + suffix;
        String queueName = "test_" + suffix;
        configureSourceConnector("http-source-connector-test_" + suffix, queueName, successTopic, errorTopic);
        configureSinkConnector("http-sink-connector-test_" + suffix,
                PUBLISH_TO_IN_MEMORY_QUEUE_OK,
                incomingTopic,
                "io.confluent.connect.json.JsonSchemaConverter", "test_" + suffix,
                new AbstractMap.SimpleImmutableEntry<>("value.converter.schema.registry.url",internalSchemaRegistryUrl),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_REQUEST_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTP_CLIENT_GENERATE_MISSING_CORRELATION_ID,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTPCLIENT_SSL_SKIP_HOSTNAME_VERIFICATION,"true"),
                new AbstractMap.SimpleImmutableEntry<>(HTTPCLIENT_DEFAULT_PROTOCOLS, "HTTP_1_1"),
                new AbstractMap.SimpleImmutableEntry<>(HTTPCLIENT_SSL_TRUSTSTORE_PATH,"/opt/"+CLIENT_TRUSTSTORE_JKS_FILENAME),
                new AbstractMap.SimpleImmutableEntry<>(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD,CLIENT_TRUSTSTORE_JKS_PASSWORD),
                new AbstractMap.SimpleImmutableEntry<>(HTTPCLIENT_SSL_TRUSTSTORE_TYPE,JKS_STORE_TYPE),
                new AbstractMap.SimpleImmutableEntry<>(HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM,TRUSTSTORE_PKIX_ALGORITHM)
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
                .register(post("/ping")
                        .willReturn(aResponse()
                                .withHeader("Content-Type","application/json")
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
        headers.put("X-Correlation-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
        headers.put("X-Request-ID",Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
        HttpRequest httpRequest = new HttpRequest(
                url,
                "POST",
                "STRING"
        );
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        Collection<Header> kafkaHeaders = Lists.newArrayList();
        ProducerRecord<String, HttpRequest> record = new ProducerRecord<>(incomingTopic, null, System.currentTimeMillis(), null, httpRequest, kafkaHeaders);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String,? extends Object> consumer = getConsumer(kafkaContainer,externalSchemaRegistryUrl);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, ? extends Object>> consumerRecords = drain(consumer, 1, 120);
        assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, ? extends Object> consumerRecord = consumerRecords.get(0);
        assertThat(consumerRecord.topic()).isEqualTo(successTopic);
        assertThat(consumerRecord.key()).isNull();
        String jsonAsString = consumerRecord.value().toString();
        LOGGER.info("json response  :{}",jsonAsString);
        String expectedJSON = "{\n" +
                "  \"durationInMillis\": 0,\n" +
                "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"request\": {\n" +
                "    \"headers\": {\n" +
                "      \"X-Correlation-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-754880687822\"\n" +
                "      ],\n" +
                "      \"X-Request-ID\": [\n" +
                "        \"e6de70d1-f222-46e8-b755-11111\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"url\": \""+baseUrl+"/ping\",\n" +
                "    \"method\": \"POST\",\n" +
                "    \"bodyType\": \"STRING\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": []\n" +
                "  },\n" +
                "  \"response\": {\n" +
                "   \"statusCode\":200,\n" +
                "  \"statusMessage\": \""+statusMessage+"\",\n" +
                "  \"headers\": {" +
                "\"Content-Type\":[\"application/json\"]" +
                "},\n" +
                "  \"body\": \""+escapedJsonResponse+"\"\n" +
                "}"+
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
        assertThat(consumerRecord.headers().toArray()).isEmpty();
    }

    private static void checkMessage(String topicName,String escapedJsonResponse, int statusCode, String statusMessage, String baseUrl, ConsumerRecord<String, ?> consumerRecord) throws JSONException {
            assertThat(consumerRecord.topic()).isEqualTo(topicName);
            assertThat(consumerRecord.key()).isNull();
            String jsonAsString = consumerRecord.value().toString();
            LOGGER.info("json response  :{}", jsonAsString);
            String expectedJSON = "{\n" +
                    "  \"durationInMillis\": 0,\n" +
                    "  \"moment\": \"2022-11-10T17:19:42.740852Z\",\n" +
                    "  \"attempts\": 1,\n" +
                    "  \"request\": {\n" +
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
                    "  \"response\": {\n" +
                    "   \"statusCode\":" + statusCode + ",\n" +
                    "  \"statusMessage\": \"" + statusMessage + "\",\n" +
                    "  \"headers\": {" +
                    "\"Content-Type\":[\"application/json\"]" +
                    "},\n" +
                    "  \"body\": \"" + escapedJsonResponse + "\"\n" +
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
            assertThat(consumerRecord.headers().toArray()).isEmpty();
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
        props.put(BOOTSTRAP_SERVERS_CONFIG,kafkaContainer.getBootstrapServers());
        props.put(SCHEMA_REGISTRY_URL_CONFIG,externalSchemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,KafkaJsonSchemaSerializer.class.getName());
        props.put(AUTO_REGISTER_SCHEMAS,"true");
        props.put(SCHEMA_SPEC_VERSION, SpecificationVersion.DRAFT_2019_09.toString());
        props.put(ONEOF_FOR_NULLABLES,"true");
        props.put(FAIL_UNKNOWN_PROPERTIES,"true");
        props.put(WRITE_DATES_AS_ISO8601,"true");
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, ? extends Object> getConsumer(
            KafkaContainer kafkaContainer,
            String schemaRegistryUrl) {

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                                                        schemaRegistryUrl,
                                                        CACHE_CAPACITY,
                                                        Lists.newArrayList(
                                                                new JsonSchemaProvider(),
                                                                new AvroSchemaProvider()),
                                                        Maps.newHashMap());
        Deserializer<String> jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient);


        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "test-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"),
                new StringDeserializer(),
                jsonSchemaDeserializer);
    }

    private List<ConsumerRecord<String, ? extends Object>> drain(KafkaConsumer<String, ? extends Object> consumer,
                                                                    int expectedRecordCount, int timeoutInSeconds) {
        List<ConsumerRecord<String, ? extends Object>> allRecords = new ArrayList<>();
        Unreliables.retryUntilTrue(timeoutInSeconds, SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);
            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }


    private static String getIP() {
        try(DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
