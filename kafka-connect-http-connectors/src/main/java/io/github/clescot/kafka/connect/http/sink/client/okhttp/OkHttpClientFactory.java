package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import io.github.clescot.kafka.connect.http.sink.client.HttpClient;
import io.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import okhttp3.Request;
import okhttp3.Response;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public class OkHttpClientFactory implements HttpClientFactory<Request, Response> {
    @Override
    public HttpClient<Request, Response> build(Map<String, Object> config, ExecutorService executorService, Random random) {
        return new OkHttpClient(config,executorService,random);
    }

}
