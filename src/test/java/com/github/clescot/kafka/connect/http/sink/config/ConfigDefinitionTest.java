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
            assert(ConfigDefinition.config().configKeys()).containsKey("connect.sink.ack.topic");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.ack.topic").name).isEqualTo("connect.sink.ack.topic");
            assertThat(ConfigDefinition.config().configKeys().get("connect.sink.ack.topic").documentation).isEqualTo("Topic to receive acknowledgment");

        }
    }
}
