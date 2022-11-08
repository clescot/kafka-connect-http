package com.github.clescot.kafka.connect.http;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.SchemaRegistryContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.github.clescot.kafka.connect.http.sink.WsSinkConfigDefinition.PUBLISH_TO_IN_MEMORY_QUEUE;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@WireMockTest
public class ITConnectorTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(ITConnectorTest.class);
    private final static Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams();
    public static final String CONFLUENT_VERSION = "7.2.2";
    public static final int CUSTOM_AVAILABLE_PORT = 0;
    public static final int CACHE_CAPACITY = 100;
    public static final String HTTP_REQUESTS = "http-requests";
    private static Network network = Network.newNetwork();
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
    @Container
    public static DebeziumContainer connectContainer = new DebeziumContainer("confluentinc/cp-kafka-connect:7.2.2")
            .withFileSystemBind("target/http-connector", "/usr/local/share/kafka/plugins")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withNetwork(network)
            .withKafka(kafkaContainer)
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
                    "org.apache.kafka.connect.runtime.distributed=ERROR," +
                    "org.apache.kafka.connect.runtime.isolation=ERROR," +
                    "org.reflections=ERROR," +
                    "org.apache.kafka.clients=ERROR")
            .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java/,/usr/share/confluent-hub-components/,/usr/local/share/kafka/plugins")
            .withExposedPorts(8083)
            .dependsOn(kafkaContainer, schemaRegistryContainer)
            .waitingFor(Wait.forHttp("/connector-plugins/"));

    private static WireMockServer wireMockServer = new WireMockServer();
    private static String hostName;
    private static String internalSchemaRegistryUrl;
    private static String externalSchemaRegistryUrl;
    private static final String successTopic = "http-success";
    private static final String errorTopic = "http-error";

    @BeforeAll
    public static void startContainers() throws IOException {
        hostName = InetAddress.getLocalHost().getHostName();

        //init wiremock
        WireMock.configureFor(hostName, CUSTOM_AVAILABLE_PORT);
        wireMockServer.start();
        org.testcontainers.Testcontainers.exposeHostPorts(wireMockServer.port());

        //start containers
        Startables.deepStart(Stream.of(kafkaContainer, schemaRegistryContainer, connectContainer)).join();
        internalSchemaRegistryUrl = "http://" + schemaRegistryContainer.getNetworkAliases().get(0) + ":8081";
        externalSchemaRegistryUrl = "http://" + schemaRegistryContainer.getHost() + ":"+schemaRegistryContainer.getMappedPort(8081);

        //register connectors
        //sink connector
        ConnectorConfiguration sinkConnectorConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.sink.WsSinkConnector")
                .with("tasks.max", "2")
                .with("topics", HTTP_REQUESTS)
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with(PUBLISH_TO_IN_MEMORY_QUEUE, "true")
                ;

        connectContainer.registerConnector("http-sink-connector", sinkConnectorConfiguration);
        connectContainer.ensureConnectorTaskState("http-sink-connector", 0, Connector.State.RUNNING);

        //source connector
        ConnectorConfiguration sourceConnectorConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.source.WsSourceConnector")
                .with("tasks.max", "2")
                .with("success.topic", successTopic)
                .with("error.topic", errorTopic)
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "io.confluent.connect.json.JsonSchemaConverter")
                .with("value.converter.schema.registry.url", internalSchemaRegistryUrl);

        connectContainer.registerConnector("http-source-connector", sourceConnectorConfiguration);
        connectContainer.ensureConnectorTaskState("http-source-connector", 0, Connector.State.RUNNING);

        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();

        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}", joinedRegisteredConnectors);
    }


    @AfterAll
    public static void afterAll() {
        wireMockServer.stop();
    }

    @AfterEach
    public void afterEach() {
        wireMockServer.resetAll();
    }

    @Test
    public void nominalCase(WireMockRuntimeInfo wmRuntimeInfo) {
        //define the http Mock Server interaction
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        wireMock
                .register(get("/ping")
                        .willReturn(aResponse()
                        .withHeader("Content-Type","application/json")
                        .withBody("{\"result\":\"pong\"}")
                        .withStatus(200)
                        .withStatusMessage("OK")
                        )
                );

        //forge messages which will command http requests
        KafkaProducer<String, String> producer = getProducer(kafkaContainer);
        Collection<Header> headers = Lists.newArrayList();
        String url = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort() + "/ping";
        LOGGER.info("url:{}", url);
        headers.add(new RecordHeader("ws-url", url.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("ws-method", "GET".getBytes(StandardCharsets.UTF_8)));
        ProducerRecord<String, String> record = new ProducerRecord<>(HTTP_REQUESTS, null, System.currentTimeMillis(), null, "value", headers);
        producer.send(record);
        producer.flush();

        //verify http responses
        KafkaConsumer<String,? extends Object> consumer = getConsumer(kafkaContainer,externalSchemaRegistryUrl);

        consumer.subscribe(Lists.newArrayList(successTopic, errorTopic));
        List<ConsumerRecord<String, ? extends Object>> consumerRecords = drain(consumer, 1);
        assertThat(consumerRecords).hasSize(1);
        ConsumerRecord<String, ? extends Object> consumerRecord = consumerRecords.get(0);
        assertThat(consumerRecord.key()).isNull();
        String jsonAsString = consumerRecord.value().toString();
        String expectedJSON = "" +
                "{\n" +
                "  \"durationInMillis\": 442,\n" +
                "  \"moment\": \"2022-11-03T18:45:01.144249Z\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"correlationId\": \"9b62eda5-7fae-42ce-b9eb-b95ef515f647\",\n" +
                "  \"requestId\": \"e6de70d1-f222-46e8-b755-754880687822\",\n" +
                "  \"requestUri\": \"http://192.168.1.4:36279/ping\",\n" +
                "  \"method\": \"GET\",\n" +
                "  \"requestHeaders\": {\n" +
                "    \"X-Request-ID\": \"e6de70d1-f222-46e8-b755-754880687822\",\n" +
                "    \"X-Correlation-ID\": \"9b62eda5-7fae-42ce-b9eb-b95ef515f647\"\n" +
                "  },\n" +
                "  \"statusCode\": 200,\n" +
                "  \"statusMessage\": \"OK\",\n" +
                "  \"responseHeaders\": {\n" +
                "    \"Transfer-Encoding\": \"chunked\",\n" +
                "    \"Matched-Stub-Id\": \"8b759c66-af77-4e33-8a4d-055ed4d91907\",\n" +
                "    \"Vary\": \"Accept-Encoding, User-Agent\",\n" +
                "    \"Content-Type\": \"application/json\"\n" +
                "  },\n" +
                "  \"responseBody\": \"{\\\"result\\\":\\\"pong\\\"}\"\n" +
                "}";
        JSONAssert.assertEquals(expectedJSON, jsonAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("correlationId", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Correlation-ID", (o1, o2) -> true),
                        new Customization("requestHeaders.X-Request-ID", (o1, o2) -> true),
                        new Customization("requestId", (o1, o2) -> true),
                        new Customization("requestUri", (o1, o2) -> true),
                        new Customization("responseHeaders.Matched-Stub-Id", (o1, o2) -> true)
                ));
        assertThat(consumerRecord.headers().toArray()).isEmpty();
//        await().atMost(Duration.ofSeconds(1000)).until(() -> Boolean.TRUE.equals(Boolean.FALSE));
    }


    private KafkaProducer<String, String> getProducer(
            KafkaContainer kafkaContainer) {

        return new KafkaProducer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaContainer.getBootstrapServers()
                ),
                new StringSerializer(),
                new StringSerializer());
    }

    private KafkaConsumer<String, ? extends Object> getConsumer(
            KafkaContainer kafkaContainer,
            String schemaRegistryUrl) {

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, CACHE_CAPACITY,Lists.newArrayList(new JsonSchemaProvider(),new AvroSchemaProvider()), Maps.newHashMap());
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

    private List<ConsumerRecord<String, ? extends Object>> drain(
            KafkaConsumer<String, ? extends Object> consumer,
            int expectedRecordCount) {

        List<ConsumerRecord<String, ? extends Object>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }


    public static void main(String[] args) throws IOException {
        ITConnectorTest itConnectorTest = new ITConnectorTest();
        String ip = itConnectorTest.getIP();
        System.out.println("ip:" + ip);
    }

    private String getIP() {
        try(DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
