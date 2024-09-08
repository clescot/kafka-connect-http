package io.github.clescot.kafka.connect.http.source;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;

import java.util.Queue;

public class HttpListener implements JobListener {

    private final Queue<HttpRequest> queue;

    public HttpListener(Queue<HttpRequest> queue) {
        this.queue = queue;
    }

    @Override
    public String getName() {
        return  "httpJobListener";
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {

    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {

    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        queue.add((HttpRequest) context.getResult());
    }
}
