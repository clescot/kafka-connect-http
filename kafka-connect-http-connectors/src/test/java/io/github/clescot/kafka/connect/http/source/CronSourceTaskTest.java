package io.github.clescot.kafka.connect.http.source;


import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.*;

import java.util.Map;

class CronSourceTaskTest {

    @Nested
    class Start {

        CronSourceTask cronSourceTask;

        @BeforeEach
        void setup() {
            cronSourceTask = new CronSourceTask();
        }


        @Test
        void test_empty_settings() {
            CronSourceTask cronSourceTask = new CronSourceTask();
            Assertions.assertThrows(ConfigException.class, () -> cronSourceTask.start(Maps.newHashMap()));
        }

        @Test
        void test_null_settings() {
            Assertions.assertThrows(NullPointerException.class, () -> cronSourceTask.start(null));
        }

        @Test
        void test_settings_with_topic_only() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            Assertions.assertThrows(ConfigException.class, () -> cronSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_1_job() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("jobs", "job10");
            settings.put("job10.cron", "0 0 6 * * ?");
            settings.put("job10.url", "https://example.com");
            Assertions.assertDoesNotThrow(() -> cronSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_2_jobs() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("jobs", "job1,job2");
            settings.put("job1.cron", "0 0 6 * * ?");
            settings.put("job1.url", "https://example.com");
            settings.put("job2.cron", "0 0 1 * * ?");
            settings.put("job2.url", "https://test.com");
            Assertions.assertDoesNotThrow(() -> cronSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_3_jobs_and_more_settings() {

            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("jobs", "job11,job22,job33");
            settings.put("job11.cron", "0 0 6 * * ?");
            settings.put("job11.url", "https://example.com");
            settings.put("job22.cron", "0 0 2 * * ?");
            settings.put("job22.url", "https://test.com");
            settings.put("job22.method", "PUT");
            settings.put("job33.cron", "0 0 2 * * ?");
            settings.put("job33.url", "https://test.com");
            settings.put("job33.method", "POST");
            settings.put("job33.body", "stuff");

            Assertions.assertDoesNotThrow(() -> cronSourceTask.start(settings));
        }

        @AfterEach
        void shutdown() {
            cronSourceTask.stop();
        }
    }

}