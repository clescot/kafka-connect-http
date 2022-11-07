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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
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
            .withNetwork(network)
//            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            ;
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(network)
            .withKafka(kafkaContainer)
//            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .dependsOn(kafkaContainer)
            .withStartupTimeout(Duration.ofSeconds(90));

    public static DebeziumContainer connectContainer =new DebeziumContainer("confluentinc/cp-kafka-connect:7.2.2")
                    .withFileSystemBind("target/http-connector", "/usr/local/share/kafka/plugins")
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                    .withNetwork(network)
//                    .withKafka(kafkaContainer)
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
                            "org.apache.kafka.clients=ERROR")
                    .withEnv("CONNECT_PLUGIN_PATH","/usr/share/java/,/usr/share/confluent-hub-components/,/usr/local/share/kafka/plugins")
//                    .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL","INFO")
//                    .withEnv("CONNECT_LOG4J_LOGGERS","org.apache.kafka.connect=DEBUG")
//                    .withEnv("GROUP_ID", "1")
//                    .withEnv("CONFIG_STORAGE_TOPIC", "debezium_connect_config")
//                    .withEnv("OFFSET_STORAGE_TOPIC", "debezium_connect_offsets")
//                    .withEnv("STATUS_STORAGE_TOPIC", "debezium_connect_status")
//                    .withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
//                    .withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                    .withExposedPorts(8083)
                    .dependsOn(kafkaContainer,schemaRegistryContainer)
                    .waitingFor(Wait.forHttp("/connector-plugins/"));


    @BeforeAll
    public static void startContainers() throws IOException {
//        createConnectorJar("target/kafka-connect-http-sink-for-it.jar");
        Startables.deepStart(Stream.of(
                        kafkaContainer,schemaRegistryContainer,connectContainer))
                .join();
    }

    @Test
    public void nominalCase(){
        ConnectorConfiguration connector = ConnectorConfiguration.create()
                .with("connector.class", "com.github.clescot.kafka.connect.http.sink.WsSinkConnector")
                .with("tasks.max", "2")
                .with("topics", "test,toto")
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
