package com.github.clescot.kafka.connect.http.sink.config;


import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@RunWith(Enclosed.class)
public class ConfigConstantsTest {

    public static class TestConfig {

        @Test
        public void test_nominal_case() {
            assertThat(ConfigConstants.TARGET_BOOTSTRAP_SERVER).isEqualTo("connect.sink.target.bootstrap.server");
            assertThat(ConfigConstants.TARGET_BOOTSTRAP_SERVER_DOC).isEqualTo("kafka target bootStrap server");
            assertThat(ConfigConstants.TARGET_SCHEMA_REGISTRY).isEqualTo("connect.sink.target.schema.registry");
            assertThat(ConfigConstants.TARGET_SCHEMA_REGISTRY_DOC).isEqualTo("Schema registry used for target kafka");
            assertThat(ConfigConstants.ACK_TOPIC).isEqualTo("connect.sink.ack.topic");
            assertThat(ConfigConstants.ACK_TOPIC_DOC).isEqualTo("Topic to receive acknowledgment");
            assertThat(ConfigConstants.ACK_SCHEMA).isEqualTo("connect.sink.ack.schema");
            assertThat(ConfigConstants.ACK_SCHEMA_DOC).isEqualTo("Schema used to send acknowledgment");



        }
    }
}
