package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import io.github.clescot.kafka.connect.http.sink.client.HttpClient;
import io.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import io.micrometer.core.instrument.MeterRegistry;
import okhttp3.Request;
import okhttp3.Response;

import java.util.Map;

public class OkHttpClientFactory implements HttpClientFactory<Request, Response> {
    @Override
    public HttpClient<Request, Response> build(Map<String, String> config) {
        return new OkHttpClient(config);
    }
}
