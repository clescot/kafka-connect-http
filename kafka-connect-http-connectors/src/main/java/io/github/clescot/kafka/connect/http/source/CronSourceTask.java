package io.github.clescot.kafka.connect.http.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.*;

import static io.github.clescot.kafka.connect.http.source.HttpJob.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class CronSourceTask extends SourceTask {

    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private CronSourceConnectorConfig cronSourceConnectorConfig;
    private Scheduler scheduler;
    private Queue<HttpRequest> queue;
    private ObjectMapper objectMapper;
    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings,"settings must not be null or empty.");
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        queue = QueueFactory.getQueue(""+UUID.randomUUID());
        Preconditions.checkNotNull(settings);
        this.cronSourceConnectorConfig = new CronSourceConnectorConfig(settings);
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        try {
            scheduler = schedulerFactory.getScheduler();
            ListenerManager listenerManager = scheduler.getListenerManager();
            listenerManager.addJobListener(new HttpListener(QueueFactory.getQueue()));
            scheduler.start();
            List<String> jobs = cronSourceConnectorConfig.getJobs();
            jobs.forEach(id -> {
                JobDataMap jobDataMap = new JobDataMap();

                String url = settings.get(id + ".url");
                jobDataMap.put(URL, url);

                Optional<String> methodAsString = Optional.ofNullable(settings.get(id + ".method"));
                methodAsString.ifPresent(method -> jobDataMap.put(METHOD, method));

                Optional<String> bodyAsString = Optional.ofNullable(settings.get(id + ".body"));
                bodyAsString.ifPresent(body -> jobDataMap.put(BODY, body));

                Optional<String> headersAsString = Optional.ofNullable(settings.get(id + ".headers"));
                List<String> headerKeys = Lists.newArrayList();
                if (headersAsString.isPresent()) {
                    headerKeys.addAll(Lists.newArrayList(headersAsString.get().split(",")));
                    headerKeys.forEach(key-> jobDataMap.put(key,settings.get(id+".header."+key)));
                }
                jobDataMap.put(HEADERS, headersAsString);

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
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            HttpRequest httpRequest = queue.poll();
            SourceRecord sourceRecord;
            try {
                sourceRecord = new SourceRecord(Maps.newHashMap(),Maps.newHashMap(),cronSourceConnectorConfig.getTopic(),null,objectMapper.writeValueAsString(httpRequest));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            records.add(sourceRecord);
        }
        return records;
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
