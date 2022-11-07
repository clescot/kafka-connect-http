package com.github.clescot.kafka.connect.http.sink.config;

public class ConfigConstants {

    protected ConfigConstants() {
        //Class with only constants
    }

    public static final String TARGET_BOOTSTRAP_SERVER = "connect.sink.target.bootstrap.server";
    public static final String TARGET_BOOTSTRAP_SERVER_DOC = "kafka target bootStrap server";

    public static final String TARGET_SCHEMA_REGISTRY = "connect.sink.target.schema.registry";
    public static final String TARGET_SCHEMA_REGISTRY_DOC = "Schema registry used for target kafka";

    public static final String ACK_TOPIC = "ack.topic";
    public static final String ACK_TOPIC_DOC = "Topic to receive acknowledgment";

    public static final String ACK_SCHEMA = "ack.schema";
    public static final String ACK_SCHEMA_DOC = "Schema used to send acknowledgment";


}
