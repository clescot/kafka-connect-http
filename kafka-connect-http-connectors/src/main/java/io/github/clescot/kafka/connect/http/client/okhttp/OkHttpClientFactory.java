package io.github.clescot.kafka.connect.http.client.okhttp;

import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;

import java.net.Proxy;
import java.net.ProxySelector;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public class OkHttpClientFactory implements HttpClientFactory<Request, Response> {
    @Override
    public HttpClient<Request, Response> build(Map<String, Object> config,
                                               ExecutorService executorService,
                                               Random random,
                                               Proxy proxy,
                                               ProxySelector proxySelector,
                                               CompositeMeterRegistry meterRegistry) {
        return new OkHttpClient(config,executorService,random,proxy,proxySelector, meterRegistry);
    }

    private okhttp3.OkHttpClient buildOkHttpClient(Map<String, Object> config,
                                                              ExecutorService executorService,
                                                              Random random,
                                                              Proxy proxy,
                                                              ProxySelector proxySelector,
                                                   CompositeMeterRegistry meterRegistry) {



        return null;
    }
}
