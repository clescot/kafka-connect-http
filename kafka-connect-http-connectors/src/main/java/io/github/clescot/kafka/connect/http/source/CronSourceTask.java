package io.github.clescot.kafka.connect.http.source;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.VersionUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.source.HttpJob.*;
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
            List<String> jobs = cronSourceConnectorConfig.getJobs();
            jobs.forEach(id-> {
                JobDataMap jobDataMap = new JobDataMap();

                String url = settings.get(id+".url");
                jobDataMap.put(URL,url);

                Optional<String> methodAsString = Optional.ofNullable(settings.get(id+".method"));
                jobDataMap.put(METHOD,methodAsString);

                Optional<String> bodyAsString = Optional.ofNullable(settings.get(id+".body"));
                jobDataMap.put(BODY,bodyAsString);

                JobDetail job = newJob(HttpJob.class)
                        .withIdentity(id)
                        .setJobData(jobDataMap)
                        .build();

                String cron = settings.get(id + ".cron");
                Trigger trigger = newTrigger()
                        .withIdentity(id)
                        .startNow()
                        .withSchedule(cronSchedule(cron))
                        .build();

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
