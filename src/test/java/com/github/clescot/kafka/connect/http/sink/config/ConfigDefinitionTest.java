package com.github.clescot.kafka.connect.http.sink.config;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(Enclosed.class)
public class ConfigDefinitionTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    public static class TestConfig {

        @Test
        public void test_nominal_case() {

            assert(ConfigDefinition.config().groups()).isEmpty();

            assert(ConfigDefinition.config().configKeys()).containsKey("connect.sink.target.bootstrap.server");
            assert(ConfigDefinition.config().configKeys()).containsKey("connect.sink.target.schema.registry");
            assert(ConfigDefinition.config().configKeys()).containsKey("connect.sink.ack.topic");
            assert(ConfigDefinition.config().configKeys()).containsKey("connect.sink.ack.schema");
            assert(ConfigDefinition.config().configKeys()).containsKey("connect.sink.producer.id");

            assertThat(ConfigDefinition.config().configKeys().values().size()).isEqualTo(6);

            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.target.bootstrap.server").name).isEqualTo("connect.sink.target.bootstrap.server");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.producer.id").name).isEqualTo("connect.sink.producer.id");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.target.schema.registry").name).isEqualTo("connect.sink.target.schema.registry");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.ack.schema").name).isEqualTo("connect.sink.ack.schema");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.ack.topic").name).isEqualTo("connect.sink.ack.topic");

            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.target.bootstrap.server").documentation).isEqualTo("kafka target bootStrap server");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.producer.id").documentation).isEqualTo("producer client id");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.target.schema.registry").documentation).isEqualTo("Schema registry used for target kafka");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.ack.schema").documentation).isEqualTo("Schema used to send acknowledgment");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.ack.topic").documentation).isEqualTo("Topic to receive acknowledgment");

        }
    }
}
