package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.Task;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.sse.core.SseEvent;

import java.util.List;

public class SseTask implements Task<OkHttpClient,SseConfiguration,HttpRequest, SseEvent> {


    @Override
    public List<SseConfiguration> getConfigurations() {
        return List.of();
    }


}
