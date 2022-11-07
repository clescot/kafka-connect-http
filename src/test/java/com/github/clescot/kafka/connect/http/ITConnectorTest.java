package com.github.clescot.kafka.connect.http;

import com.google.common.base.Joiner;
import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.debezium.testing.testcontainers.SchemaRegistryContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.time.Duration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;

@Testcontainers
public class ITConnectorTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(ITConnectorTest.class);
    private final static Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams();
    public static final String CONFLUENT_VERSION = "7.2.2";
    private static Network network = Network.newNetwork();
    @Container
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:"+CONFLUENT_VERSION))
            .withNetwork(network);
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer)
            .withStartupTimeout(Duration.ofSeconds(90));

    public static DebeziumContainer connectContainer =new DebeziumContainer("debezium/connect:2.1")
                    .withFileSystemBind("target/http-connector/kafka-connect-http-sink.jar", "/kafka/connect/http-connector/kafka-connect-http-sink.jar")
                    .withNetwork(network)
                    .withKafka(kafkaContainer)
                    .dependsOn(kafkaContainer,schemaRegistryContainer);


    @BeforeAll
    public static void startContainers() throws IOException {
//        createConnectorJar("target/kafka-connect-http-sink-for-it.jar");
        Startables.deepStart(Stream.of(
                        kafkaContainer,schemaRegistryContainer, connectContainer))
                .join();
        kafkaContainer.followOutput(logConsumer);
        schemaRegistryContainer.followOutput(logConsumer);
        connectContainer.followOutput(logConsumer);
    }

    @Test
    public void nominalCase(){
        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.sink.WsSinkConnector")
                .with("tasks.max", "2")
                .with("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .with("value.converter", "org.apache.kafka.connect.storage.StringConverter");


        connectContainer.registerConnector("http-connector", connector);
        connectContainer.ensureConnectorTaskState("http-connector", 0, Connector.State.RUNNING);

        List<String> registeredConnectors = connectContainer.getRegisteredConnectors();

        String joinedRegisteredConnectors = Joiner.on(",").join(registeredConnectors);
        LOGGER.info("registered connectors :{}",joinedRegisteredConnectors);
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!:"+joinedRegisteredConnectors);
    }

    private static void createConnectorJar(String jarPath) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        JarOutputStream target = new JarOutputStream(new FileOutputStream(jarPath), manifest);
        add(new File("target/classes"), target);
        target.close();
    }

    private static void add(File source, JarOutputStream target) throws IOException {
        String name = source.getPath().replace("\\", "/").replace("target/classes/", "");
        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (File nestedFile : source.listFiles()) {
                add(nestedFile, target);
            }
        }
        else {
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(source))) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1)
                        break;
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }
}
