package io.github.clescot.kafka.connect.http.sink.client;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.Map;

public interface HttpClientFactory {
    HttpClient build(Map<String, String> config, MeterRegistry meterRegistry);
}
