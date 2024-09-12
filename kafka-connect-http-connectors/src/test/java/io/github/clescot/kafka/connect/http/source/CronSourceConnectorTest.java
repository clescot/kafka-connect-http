package io.github.clescot.kafka.connect.http.source;


import org.gradle.internal.impldep.com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class CronSourceConnectorTest {

    @Nested
    class Version{
        @Test
        void nominal_case(){
            CronSourceConnector cronSourceConnector = new CronSourceConnector();
            String version = cronSourceConnector.version();
            assertThat(version).isNotNull().isNotBlank();
        }
    }


    @Nested
    class TaskConfigs{
        private CronSourceConnector cronSourceConnector;
        @BeforeEach
        void setup(){
            cronSourceConnector = new CronSourceConnector();
            HashMap<String, String> settings = Maps.newHashMap();
            settings.put("topic","test");
            settings.put("jobs","job1");
            cronSourceConnector.start(settings);
        }
        @Test
        void nominal_case(){
            Assertions.assertDoesNotThrow(()->cronSourceConnector.taskConfigs(1));
        }
        @Test
        void test_0_tasks(){
            Assertions.assertThrows(IllegalArgumentException.class,()->cronSourceConnector.taskConfigs(0));
        }

        @Test
        void test_negative_tasks(){
            Assertions.assertThrows(IllegalArgumentException.class,()->cronSourceConnector.taskConfigs(-1));
        }
    }
}