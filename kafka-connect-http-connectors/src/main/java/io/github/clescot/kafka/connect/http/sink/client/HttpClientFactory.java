package io.github.clescot.kafka.connect.http.sink.client;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public interface HttpClientFactory<Req,Res> {
    HttpClient<Req,Res> build(Map<String, String> config, ExecutorService executorService);
}
