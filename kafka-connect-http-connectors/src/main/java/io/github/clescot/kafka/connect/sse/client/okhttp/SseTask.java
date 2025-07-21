package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.Task;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.sse.core.SseEvent;

public class SseTask extends Task<OkHttpClient,HttpRequest, SseEvent> {
    @Override
    public Configuration<OkHttpClient,HttpRequest> selectConfiguration(HttpRequest request) {
        return null;
    }
}
