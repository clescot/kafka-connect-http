package io.github.clescot.kafka.connect.http.client;

import io.github.clescot.kafka.connect.http.client.proxy.ProxySelectorFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public interface HttpClientFactory<R,S> {

    Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);
    HttpClient<R,S> build(Map<String, Object> config,
                              ExecutorService executorService,
                              Random random,
                              Proxy proxy,
                              ProxySelector proxySelector, CompositeMeterRegistry meterRegistry);

    default HttpClient<R,S> buildHttpClient(Map<String, Object> config,
                                                  ExecutorService executorService,
                                                  CompositeMeterRegistry meterRegistry, Random random) {

        //get proxy
        Proxy proxy = null;
        if (config.containsKey(PROXY_HTTP_CLIENT_HOSTNAME)) {
            String proxyHostName = (String) config.get(PROXY_HTTP_CLIENT_HOSTNAME);
            int proxyPort = (Integer) config.get(PROXY_HTTP_CLIENT_PORT);
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = (String) Optional.ofNullable(config.get(PROXY_HTTP_CLIENT_TYPE)).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            proxy = new Proxy(proxyType, socketAddress);
        }

        ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
        ProxySelector proxySelector = null;
        if (config.get(PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME) != null) {
            proxySelector = proxySelectorFactory.build(config, random);
        }
        return build(config, executorService, random, proxy, proxySelector, meterRegistry);
    }



}
