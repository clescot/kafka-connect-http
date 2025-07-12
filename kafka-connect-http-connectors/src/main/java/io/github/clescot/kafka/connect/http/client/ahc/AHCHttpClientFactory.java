package io.github.clescot.kafka.connect.http.client.ahc;

import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Proxy;
import java.net.ProxySelector;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public class AHCHttpClientFactory implements HttpClientFactory<AHCHttpClient,Request, Response> {


    private static final Logger LOGGER = LoggerFactory.getLogger(AHCHttpClientFactory.class);





    @Override
    public AHCHttpClient build(Map<String, Object> config,
                               ExecutorService executorService,
                               Random random,
                               Proxy proxy,
                               ProxySelector proxySelector, CompositeMeterRegistry meterRegistry) {
        //executorService is not used for AHC : we cannot set an executorService nor a thread pool to AHC
        return new AHCHttpClient(config);
    }

}

