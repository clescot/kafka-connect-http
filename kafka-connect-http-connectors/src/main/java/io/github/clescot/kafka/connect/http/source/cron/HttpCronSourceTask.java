package io.github.clescot.kafka.connect.http.source.cron;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.*;

import static io.github.clescot.kafka.connect.http.core.VersionUtils.VERSION;
import static io.github.clescot.kafka.connect.http.source.cron.HttpCronJob.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class HttpCronSourceTask extends SourceTask {

    public static final String JOB_PREFIX = "job.";
    private Scheduler scheduler;
    private Queue<HttpRequest> queue;
    private ObjectMapper objectMapper;
    private Map<String, String> settings;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings,"settings must not be null or empty.");
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        queue = QueueFactory.getQueue(""+UUID.randomUUID());
        Preconditions.checkNotNull(settings);
        this.settings = settings;
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        try {
            scheduler = schedulerFactory.getScheduler();
            ListenerManager listenerManager = scheduler.getListenerManager();
            listenerManager.addJobListener(new HttpCronJobListener(queue));
            scheduler.start();
            String jobIds = Optional.ofNullable(settings.get(HttpCronSourceConfigDefinition.JOBS)).orElseThrow(()-> new IllegalArgumentException("jobs configuration is required"));
            List<String> jobs = Arrays.asList(jobIds.split(","));
            jobs.forEach(jobId -> {
                JobDataMap jobDataMap = new JobDataMap();

                String url = settings.get(JOB_PREFIX +jobId + ".url");
                jobDataMap.put(URL, url);

                Optional<String> methodAsString = Optional.ofNullable(settings.get(JOB_PREFIX +jobId + ".method"));
                methodAsString.ifPresent(method -> jobDataMap.put(METHOD, method));

                Optional<String> bodyAsString = Optional.ofNullable(settings.get(JOB_PREFIX +jobId + ".body"));
                bodyAsString.ifPresent(body -> jobDataMap.put(BODY, body));

                Optional<String> headersAsString = Optional.ofNullable(settings.get(JOB_PREFIX +jobId + ".headers"));
                List<String> headerKeys = Lists.newArrayList();
                if (headersAsString.isPresent()) {
                    headerKeys.addAll(Lists.newArrayList(headersAsString.get().split(",")));
                    headerKeys.forEach(key-> jobDataMap.put(key,settings.get(JOB_PREFIX +jobId+".header."+key)));
                    jobDataMap.put(HEADERS, headersAsString.get());
                }

                JobDetail job = newJob(HttpCronJob.class)
                        .withIdentity(jobId)
                        .setJobData(jobDataMap)
                        .build();

                String cron = settings.get(JOB_PREFIX +jobId + ".cron");
                Trigger trigger = newTrigger()
                        .withIdentity(jobId)
                        .startNow()
                        .withSchedule(cronSchedule(cron))
                        .build();

                try {
                    scheduler.scheduleJob(job, trigger);
                } catch (SchedulerException e) {
                    throw new CronException(e);
                }
            });

        } catch (SchedulerException e) {
            throw new CronException(e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            HttpRequest httpRequest = queue.poll();
            SourceRecord sourceRecord;
            try {
                sourceRecord = new SourceRecord(
                        Maps.newHashMap(),
                        Maps.newHashMap(),
                        settings.get(HttpCronSourceConfigDefinition.TOPIC),
                        null,
                        objectMapper.writeValueAsString(httpRequest)
                );
            } catch (JsonProcessingException e) {
                throw new CronException(e);
            }
            records.add(sourceRecord);
        }
        return records;
    }

    @Override
    public void stop() {
        try {
            if(scheduler!=null) {
                scheduler.shutdown(true);
            }
        } catch (SchedulerException e) {
            throw new CronException(e);
        }
    }

    protected Scheduler getScheduler() {
        return scheduler;
    }

    protected Queue<HttpRequest> getQueue(){
        return queue;
    }
}
