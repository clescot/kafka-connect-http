package io.github.clescot.kafka.connect.http;/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    private static final String SCHEMA_REGISTRY_DOCKER_IMAGE_NAME = "confluentinc/cp-schema-registry:7.3.0";
    private static final Integer SCHEMA_REGISTRY_EXPOSED_PORT = 8081;
    public static final Integer KAFKA_EXPOSED_PORT = 9092;

    public SchemaRegistryContainer() {
        this(SCHEMA_REGISTRY_DOCKER_IMAGE_NAME);
    }

    public SchemaRegistryContainer(String dockerImage) {
        super(dockerImage);
        addExposedPorts(SCHEMA_REGISTRY_EXPOSED_PORT);
    }

    public SchemaRegistryContainer withKafka(KafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":"+ KAFKA_EXPOSED_PORT);
    }

    public SchemaRegistryContainer withKafka(Network network, String bootstrapServers) {
        withNetwork(network);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081");
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return self();
    }
}
