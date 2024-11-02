package io.github.clescot.kafka.connect.http.source.cron;

import io.github.clescot.kafka.connect.http.core.HttpRequest;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CronJobConfig {

    private final String id;
    private final String url;
    private final String cronExpression;
    private final HttpRequest.Method method;
    private final String body;
    private final Map<String, List<String>> headers;

    public CronJobConfig(String id, String url, String cronExpression, HttpRequest.Method method, String body, Map<String, List<String>> headers) {
        this.id = id;
        this.url = url;
        this.cronExpression = cronExpression;
        this.method = Optional.ofNullable(method).orElse(HttpRequest.Method.GET);
        this.body = body;
        this.headers = headers;
    }

    public String getUrl() {
        return url;
    }

    public String getBody() {
        return body;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public String getId() {
        return id;
    }

    public HttpRequest.Method getMethod() {
        return method;
    }
}
