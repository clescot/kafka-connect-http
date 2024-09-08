package io.github.clescot.kafka.connect.http.source;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.util.Optional;

/**
 * build an HttpRequest.
 */
public class HttpJob implements Job {

    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String BODY = "body";

    @Override
    public void execute(JobExecutionContext context) {
        String url = (String) context.get(URL);
        Optional<String> methodAsString = Optional.ofNullable((String) context.get(METHOD));
        HttpRequest.Method method = HttpRequest.Method.valueOf(methodAsString.orElse("GET"));
        String body = (String)context.get(BODY);
        HttpRequest httpRequest = new HttpRequest(url, method);
        if(body!=null && !body.isBlank()){
            httpRequest.setBodyAsString(body);
        }
        context.setResult(httpRequest);
    }
}
