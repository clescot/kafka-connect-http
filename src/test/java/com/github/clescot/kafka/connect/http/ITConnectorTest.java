package com.github.clescot.kafka.connect.http;

import com.github.tomakehurst.wiremock.client.WireMock;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.awaitility.Awaitility.await;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.common.base.Joiner;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.SchemaRegistryContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
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

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

@Testcontainers
@WireMockTest
public class ITConnectorTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(ITConnectorTest.class);
    private final static Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams();
    public static final String CONFLUENT_VERSION = "7.2.2";
    private static Network network = Network.newNetwork();
    @Container
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:"+CONFLUENT_VERSION))
            .withNetwork(network)
//            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            ;
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer)
            .withStartupTimeout(Duration.ofSeconds(90));

    public static DebeziumContainer connectContainer =new DebeziumContainer("confluentinc/cp-kafka-connect:7.2.2")
                    .withFileSystemBind("target/http-connector", "/usr/local/share/kafka/plugins")
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .withEnv("CONNECT_BOOTSTRAP_SERVERS",kafkaContainer.getNetworkAliases().get(0) + ":9092")
                    .withEnv("CONNECT_GROUP_ID","test")
                    .withEnv("CONNECT_CONFIG_STORAGE_TOPIC","test_config")
                    .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR","1")
                    .withEnv("CONNECT_OFFSET_STORAGE_TOPIC","test_offset")
                    .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR","1")
                    .withEnv("CONNECT_STATUS_STORAGE_TOPIC","test_status")
                    .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR","1")
                    .withEnv("CONNECT_KEY_CONVERTER","org.apache.kafka.connect.storage.StringConverter")
                    .withEnv("CONNECT_VALUE_CONVERTER","org.apache.kafka.connect.storage.StringConverter")
                    .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME","pop-os.localdomain")
                    .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL","INFO")
                    .withEnv("CONNECT_LOG4J_LOGGERS","" +
                            "org.apache.kafka.connect=ERROR," +
                            "org.apache.kafka.connect.runtime.distributed=ERROR," +
                            "org.apache.kafka.connect.runtime.isolation=DEBUG," +
                            "org.reflections=ERROR," +
                            "org.apache.kafka.clients=ERROR")
                    .withEnv("CONNECT_PLUGIN_PATH","/usr/share/java/,/usr/share/confluent-hub-components/,/usr/local/share/kafka/plugins")
                    .withExposedPorts(8083)
                    .dependsOn(kafkaContainer,schemaRegistryContainer)
                    .waitingFor(Wait.forHttp("/connector-plugins/"));


    @BeforeAll
    public static void startContainers() throws IOException {
        Startables.deepStart(Stream.of(kafkaContainer,schemaRegistryContainer,connectContainer)).join();
    }

    @Test
    public void nominalCase(WireMockRuntimeInfo wmRuntimeInfo){
        ConnectorConfiguration sinkConnectorConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.sink.WsSinkConnector")
                .with("tasks.max", "2")
                .with("topics", "http-requests")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "io.confluent.connect.json.JsonSchemaConverter")
                .with("value.converter.schema.registry.url","http://"+schemaRegistryContainer.getHost()+schemaRegistryContainer.getMappedPort(8081));
        ConnectorConfiguration sourceConnectorConfiguration = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.source.WsSourceConnector")
                .with("tasks.max", "2")
                .with("topics", "http-responses")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        connectContainer.registerConnector("http-connector", sinkConnectorConfiguration);
        connectContainer.ensureConnectorTaskState("http-connector", 0, Connector.State.RUNNING);

        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();

        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}",joinedRegisteredConnectors);
        String httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
        LOGGER.info("wiremock : {}",httpBaseUrl);
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        wireMock.register(get("/ping").willReturn(aResponse().withBody("pong").withStatus(200).withStatusMessage("OK")));
        KafkaProducer<String, String> producer = getProducer(kafkaContainer);
        ProducerRecord<String,String> record = new ProducerRecord<>("http-requests","value");
        producer.send(record);
        producer.flush();
        await().atMost(Duration.ofSeconds(1000)).until(()->Boolean.TRUE.equals(Boolean.FALSE));
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

    private KafkaConsumer<String, String> getConsumer(
            KafkaContainer kafkaContainer) {

        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "test-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private List<ConsumerRecord<String, String>> drain(
            KafkaConsumer<String, String> consumer,
            int expectedRecordCount) {

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }

}
