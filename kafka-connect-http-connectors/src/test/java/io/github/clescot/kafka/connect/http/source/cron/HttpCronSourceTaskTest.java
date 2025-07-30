package io.github.clescot.kafka.connect.http.source.cron;


import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.quartz.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class HttpCronSourceTaskTest {

    @Nested
    class Start {

        HttpCronSourceTask httpCronSourceTask;

        @BeforeEach
        void setup() {
            httpCronSourceTask = new HttpCronSourceTask();
        }


        @Test
        void test_empty_settings() {
            HashMap<String, String> settings = Maps.newHashMap();
            Assertions.assertThrows(ConfigException.class, () -> httpCronSourceTask.start(settings));
        }

        @Test
        void test_null_settings() {
            Assertions.assertThrows(NullPointerException.class, () -> httpCronSourceTask.start(null));
        }

        @Test
        void test_settings_with_topic_only() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            Assertions.assertThrows(ConfigException.class, () -> httpCronSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_1_job() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("jobs", "job10");
            settings.put("job.job10.cron", "0 0 6 * * ?");
            settings.put("job.job10.url", "https://example.com");
            Assertions.assertDoesNotThrow(() -> httpCronSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_2_jobs() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("jobs", "job1,job2");
            settings.put("job.job1.cron", "0 0 6 * * ?");
            settings.put("job.job1.url", "https://example.com");
            settings.put("job.job2.cron", "0 0 1 * * ?");
            settings.put("job.job2.url", "https://test.com");
            Assertions.assertDoesNotThrow(() -> httpCronSourceTask.start(settings));
        }

        @Test
        void test_settings_with_topic_and_3_jobs_and_more_settings() throws SchedulerException {

            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("jobs", "job11,job22,job33");
            settings.put("job.job11.cron", "0 0 6 * * ?");
            settings.put("job.job11.url", "https://example.com");
            settings.put("job.job22.cron", "0 0 2 * * ?");
            settings.put("job.job22.url", "https://test.com");
            settings.put("job.job22.method", "PUT");
            settings.put("job.job33.cron", "0 0 2 * * ?");
            settings.put("job.job33.url", "https://test.com");
            settings.put("job.job33.method", "POST");
            settings.put("job.job33.body", "stuff");

            Assertions.assertDoesNotThrow(() -> httpCronSourceTask.start(settings));
            Scheduler scheduler = httpCronSourceTask.getScheduler();
            JobDetail jobDetail = scheduler.getJobDetail(new JobKey("job11"));
            Class<? extends Job> jobClass = jobDetail.getJobClass();
            assertThat(jobClass).isEqualTo(HttpCronJob.class);
            JobDataMap jobDataMap = jobDetail.getJobDataMap();
            assertThat(jobDataMap).containsEntry("url", "https://example.com");

            JobDetail jobDetail2 = scheduler.getJobDetail(new JobKey("job22"));
            JobDataMap jobDataMap2 = jobDetail2.getJobDataMap();
            assertThat(jobDataMap2)
                    .containsEntry("url", "https://test.com")
                    .containsEntry("method", "PUT");

            JobDetail jobDetail3 = scheduler.getJobDetail(new JobKey("job33"));
            JobDataMap jobDataMap3 = jobDetail3.getJobDataMap();
            assertThat(jobDataMap3)
                    .containsEntry("url", "https://test.com")
                    .containsEntry("method", "POST")
                    .containsEntry("body", "stuff");

        }

        @AfterEach
        void shutdown() {
            httpCronSourceTask.stop();
        }
    }


    @Nested
    class Version {

        @Test
        void get_version() {
            HttpCronSourceTask httpCronSourceTask = new HttpCronSourceTask();
            String version = httpCronSourceTask.version();
            assertThat(version)
                    .isNotNull()
                    .isNotBlank();
        }
    }

    @Nested
    class Poll{
        HttpCronSourceTask httpCronSourceTask;

        @BeforeEach
        void setup() {
            httpCronSourceTask = new HttpCronSourceTask();
        }

        @AfterEach
        void shutdown() {
            httpCronSourceTask.stop();
        }

        @Test
        void test_nominal_case() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("topic", "test");
            settings.put("jobs", "job11,job22,job33");
            settings.put("job.job11.cron", "* * * * * ?");
            settings.put("job.job11.url", "https://example.com");
            settings.put("job.job22.cron", "0 0 2 * * ?");
            settings.put("job.job22.url", "https://test.com");
            settings.put("job.job22.method", "PUT");
            settings.put("job.job33.cron", "0 0 2 * * ?");
            settings.put("job.job33.url", "https://test.com");
            settings.put("job.job33.method", "POST");
            settings.put("job.job33.body", "stuff");
            httpCronSourceTask.start(settings);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()-> httpCronSourceTask.getQueue().peek()!=null);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(()->!httpCronSourceTask.poll().isEmpty());

        }
    }

}