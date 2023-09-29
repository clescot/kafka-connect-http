package io.github.clescot.kafka.connect.http.client;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.net.Proxy;
import java.net.ProxySelector;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public interface HttpClientFactory<Req,Res> {
    HttpClient<Req,Res> build(Map<String, Object> config,
                              ExecutorService executorService,
                              Random random,
                              Proxy proxy,
                              ProxySelector proxySelector, CompositeMeterRegistry meterRegistry);
}
