package io.github.clescot.kafka.connect.http.source.cron;


import org.gradle.internal.impldep.com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            settings.put("jobs","job1,job2");
            settings.put("job.job1.url","http://localhost:8080/test");
            settings.put("job.job1.cron","0 0/1 * * * ?");
            settings.put("job.job1.method","GET");
            settings.put("job.job1.body","{\"key\":\"value\"}");
            settings.put("job.job2.url","http://localhost:8080/test2");
            settings.put("job.job2.cron","0 0/1 * * * ?");
            settings.put("job.job2.method","POST");
            settings.put("job.job2.body","{\"key\":\"value\"}");
            cronSourceConnector.start(settings);
        }
        @Test
        void nominal_case(){
            List<Map<String, String>> list = cronSourceConnector.taskConfigs(1);
            assertThat(list).hasSize(1);
            Map<String, String> taskConfig = list.get(0);
            assertThat(taskConfig).isNotNull();
            assertThat(taskConfig).hasSize(9);
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