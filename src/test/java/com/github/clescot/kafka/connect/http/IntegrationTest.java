package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.sink.WsSinkTask;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.SchemaRegistryContainer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

@Testcontainers
public class IntegrationTest {


    private final static Logger LOGGER = LoggerFactory.getLogger(IntegrationTest.class);
    public static final String CONFLUENT_VERSION = "7.2.2";
    private static Network network = Network.newNetwork();
    @Container
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:"+CONFLUENT_VERSION)).withNetwork(network);
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer)
            .withStartupTimeout(Duration.ofSeconds(90));

    public static DebeziumContainer debeziumContainer =new DebeziumContainer("debezium/connect:2.1")
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .dependsOn(kafkaContainer,schemaRegistryContainer);


    @BeforeClass
    public static void startContainers() {
        Startables.deepStart(Stream.of(
                        kafkaContainer,schemaRegistryContainer, debeziumContainer))
                .join();
    }

    @Test
    public void nominalCase(){

        List<String> registeredConnectors = debeziumContainer.getRegisteredConnectors();
        LOGGER.info("registered connectors :{}",registeredConnectors);
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    }
}
