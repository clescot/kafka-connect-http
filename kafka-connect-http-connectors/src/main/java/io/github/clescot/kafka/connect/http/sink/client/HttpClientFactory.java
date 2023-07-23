package io.github.clescot.kafka.connect.http.sink.client;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public interface HttpClientFactory<Req,Res> {
    HttpClient<Req,Res> build(Map<String, Object> config, ExecutorService executorService, Random random);
}
