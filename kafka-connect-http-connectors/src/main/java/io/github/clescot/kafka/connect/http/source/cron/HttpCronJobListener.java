package io.github.clescot.kafka.connect.http.source.cron;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class HttpCronJobListener implements JobListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpCronJobListener.class);
    private final Queue<HttpRequest> queue;

    public HttpCronJobListener(Queue<HttpRequest> queue) {
        this.queue = queue;
    }

    @Override
    public String getName() {
        return "httpJobListener";
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        //nothing to do
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
        //nothing to do
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        HttpRequest result = (HttpRequest) context.getResult();
        LOGGER.debug("new request: {}",result);
        queue.add(result);
    }
}
