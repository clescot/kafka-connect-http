package io.github.clescot.kafka.connect.http.source;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * build an HttpRequest.
 */
public class HttpJob implements Job {

    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String BODY = "body";
    public static final String HEADERS = "headers";

    @Override
    public void execute(JobExecutionContext context) {
        JobDataMap jobDataMap = context.getMergedJobDataMap();
        String url = jobDataMap.getString(URL);
        Optional<String> methodAsString = Optional.ofNullable(jobDataMap.getString(METHOD));
        HttpRequest.Method method = HttpRequest.Method.valueOf(methodAsString.orElse("GET"));
        String body = jobDataMap.getString(BODY);
        HttpRequest httpRequest = new HttpRequest(url, method);
        if(body!=null && !body.isBlank()){
            httpRequest.setBodyAsString(body);
        }
        Optional<String> headersIdsAsString = Optional.ofNullable(jobDataMap.getString(HEADERS));
        List<String> headerIds = Lists.newArrayList();
        if(headersIdsAsString.isPresent()){
            headersIdsAsString.ifPresent(ids ->headerIds.addAll(Lists.newArrayList(ids.split(","))));
        }
        Map<String,List<String>> headers = Maps.newHashMap();
        headerIds.forEach(id-> headers.put(id,Lists.newArrayList(((String)jobDataMap.get(id)).split(","))));
        httpRequest.setHeaders(headers);
        context.setResult(httpRequest);
    }
}
