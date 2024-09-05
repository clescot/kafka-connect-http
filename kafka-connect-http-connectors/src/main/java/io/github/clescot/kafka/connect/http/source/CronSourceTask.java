package io.github.clescot.kafka.connect.http.source;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.List;
import java.util.Map;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class CronSourceTask extends SourceTask {

    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private CronSourceConnectorConfig cronSourceConnectorConfig;
    private Scheduler scheduler;
    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings);
        this.cronSourceConnectorConfig = new CronSourceConnectorConfig(settings);
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        try {
            scheduler = schedulerFactory.getScheduler();
            scheduler.start();
            List<Pair<CronExpression, HttpRequest>> jobs = cronSourceConnectorConfig.getJobs();
            jobs.forEach(pair-> {
                // define the job and tie it to our HttpJob class
                JobDetail job = newJob(HttpJob.class)
                        .withIdentity("job1")
                        .build();
                // Trigger the job to run now, and then repeat every 40 seconds
                Trigger trigger = newTrigger()
                        .withIdentity("trigger1", "group1")
                        .startNow()
                        .withSchedule(cronSchedule("* * * * ? *"))
                        .build();

                // Tell quartz to schedule the job using our trigger
                try {
                    scheduler.scheduleJob(job, trigger);
                } catch (SchedulerException e) {
                    throw new RuntimeException(e);
                }
            });

        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return List.of();
    }

    @Override
    public void stop() {
        try {
            scheduler.shutdown(true);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
