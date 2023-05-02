package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import io.github.clescot.kafka.connect.http.sink.client.HttpClient;
import io.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;

public class OkHttpClientFactory implements HttpClientFactory {
    @Override
    public HttpClient build(Map<String, String> config) {
        return new OkHttpClient(config);
    }
}
