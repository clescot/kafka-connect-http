package io.github.clescot.kafka.connect.http.source;


import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.*;
import org.quartz.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
        void test_settings_with_topic_and_3_jobs_and_more_settings() throws SchedulerException {

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
            Scheduler scheduler = cronSourceTask.getScheduler();
            JobDetail jobDetail = scheduler.getJobDetail(new JobKey("job11"));
            Class<? extends Job> jobClass = jobDetail.getJobClass();
            assertThat(jobClass).isEqualTo(HttpJob.class);
            JobDataMap jobDataMap = jobDetail.getJobDataMap();
            assertThat(jobDataMap).containsEntry("url", "https://example.com");

            JobDetail jobDetail2 = scheduler.getJobDetail(new JobKey("job22"));
            JobDataMap jobDataMap2 = jobDetail2.getJobDataMap();
            assertThat(jobDataMap2).containsEntry("url", "https://test.com");
            assertThat(jobDataMap2).containsEntry("method", "PUT");

            JobDetail jobDetail3 = scheduler.getJobDetail(new JobKey("job33"));
            JobDataMap jobDataMap3 = jobDetail3.getJobDataMap();
            assertThat(jobDataMap3).containsEntry("url", "https://test.com");
            assertThat(jobDataMap3).containsEntry("method", "POST");
            assertThat(jobDataMap3).containsEntry("body", "stuff");

        }

        @AfterEach
        void shutdown() {
            cronSourceTask.stop();
        }
    }


    @Nested
    class Version {

        @Test
        void get_version() {
            CronSourceTask cronSourceTask = new CronSourceTask();
            String version = cronSourceTask.version();
            assertThat(version).isNotNull();
            assertThat(version).isNotBlank();
        }
    }

}