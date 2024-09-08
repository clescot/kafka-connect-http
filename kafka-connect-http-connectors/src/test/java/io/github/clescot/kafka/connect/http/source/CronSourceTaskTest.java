package io.github.clescot.kafka.connect.http.source;


import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

class CronSourceTaskTest {

    @Nested
    class Start{


        @Test
        void test_empty_settings(){
            CronSourceTask cronSourceTask = new CronSourceTask();
            Assertions.assertThrows(ConfigException.class,()->cronSourceTask.start(Maps.newHashMap()));
        }

        @Test
        void test_null_settings(){
            CronSourceTask cronSourceTask = new CronSourceTask();
            Assertions.assertThrows(NullPointerException.class,()->cronSourceTask.start(null));
        }

        @Test
        void test_settings_with_topic_only(){
            CronSourceTask cronSourceTask = new CronSourceTask();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic","test");
            Assertions.assertThrows(ConfigException.class,()->cronSourceTask.start(settings));
        }
        @Test
        void test_settings_with_topic_and_1_job(){
            CronSourceTask cronSourceTask = new CronSourceTask();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic","test");
            settings.put("jobs","job1");
            settings.put("job1.cron","0 0 6 * * ?");
            settings.put("job1.url","https://example.com");
            Assertions.assertDoesNotThrow(()->cronSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_2_jobs(){
            CronSourceTask cronSourceTask = new CronSourceTask();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic","test");
            settings.put("jobs","job1,job2");
            settings.put("job1.cron","0 0 6 * * ?");
            settings.put("job1.url","https://example.com");
            settings.put("job2.cron","0 0 6 * * ?");
            settings.put("job2.url","https://test.com");
            Assertions.assertDoesNotThrow(()->cronSourceTask.start(settings));
        }
    }

}