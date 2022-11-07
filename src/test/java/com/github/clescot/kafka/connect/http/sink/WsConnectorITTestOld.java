package com.github.clescot.kafka.connect.http.sink;

import com.github.ydespreaux.testcontainers.kafka.containers.KafkaConnectContainer;
import com.github.ydespreaux.testcontainers.kafka.rule.ConfluentKafkaContainer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.testcontainers.containers.MSSQLServerContainer;

public class WsConnectorITTestOld {
    @ClassRule
    public static final ConfluentKafkaContainer kafkaContainer = new ConfluentKafkaContainer().withSchemaRegistry(true);

    @Rule
    public KafkaConnectContainer kafkaConnectContainer = new KafkaConnectContainer("5.3.1").withBrokersServerUrl(kafkaContainer.getBootstrapServers());

    @Rule
    public MSSQLServerContainer mssqlServerContainer = new MSSQLServerContainer();
//    @Test
    public void test(){
        kafkaContainer.start();
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        System.out.println("bootstrapServers="+bootstrapServers);
        kafkaConnectContainer.withSchemaRegistryUrl(kafkaContainer.getSchemaRegistryContainer().getURL());
        kafkaConnectContainer.start();

    }
}
