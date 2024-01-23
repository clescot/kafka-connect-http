package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import dev.failsafe.RateLimiterConfig;
import io.github.clescot.kafka.connect.http.client.proxy.ProxySelectorFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static io.github.clescot.kafka.connect.http.client.Configuration.CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.Configuration.STATIC_SCOPE;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public interface HttpClientFactory<R,S> {

    Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);
    //rate limiter
    Map<String, RateLimiter<HttpExchange>> sharedRateLimiters = Maps.newHashMap();
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
        HttpClient<R, S> httpClient = build(config, executorService, random, proxy, proxySelector, meterRegistry);
        httpClient.setRateLimiter(buildRateLimiter(config));
        return httpClient;
    }

    private RateLimiter<HttpExchange> buildRateLimiter(Map<String, Object> configMap) {
        String configurationId = (String) configMap.get(CONFIGURATION_ID);
        RateLimiter<HttpExchange> rateLimiter = null;
        if (configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)) {
            long maxExecutions = Long.parseLong((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            LOGGER.trace("configuration '{}' : maxExecutions :{}",configurationId,maxExecutions);
            long periodInMs = Long.parseLong(Optional.ofNullable((String) configMap.get(RATE_LIMITER_PERIOD_IN_MS)).orElse(1000 + ""));
            LOGGER.trace("configuration '{}' : periodInMs :{}",configurationId,periodInMs);
            if (configMap.containsKey(RATE_LIMITER_SCOPE) && STATIC_SCOPE.equalsIgnoreCase((String) configMap.get(RATE_LIMITER_SCOPE))) {
                LOGGER.trace("configuration '{}' : rateLimiter scope is 'static'",configurationId);
                Optional<RateLimiter<HttpExchange>> sharedRateLimiter = Optional.ofNullable(sharedRateLimiters.get(configurationId));
                if (sharedRateLimiter.isPresent()) {
                    rateLimiter = sharedRateLimiter.get();
                    LOGGER.trace("configuration '{}' : rate limiter is already shared",configurationId);
                } else {
                    rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
                    registerRateLimiter(configurationId, rateLimiter);
                }
            } else {
                rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
            }
            RateLimiterConfig<HttpExchange> config = rateLimiter.getConfig();
            LOGGER.trace("configuration '{}' rate limiter configured : 'maxRate:{},maxPermits{},period:{},maxWaitTime:{}'",configurationId,config.getMaxRate(),config.getMaxPermits(),config.getPeriod(),config.getMaxWaitTime());
        }else{
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("configuration '{}' : rate limiter is not configured", configurationId);
                LOGGER.trace(Joiner.on(",\n").withKeyValueSeparator("=").join(configMap.entrySet()));
            }
        }
        return rateLimiter;
    }


    public static void registerRateLimiter(String configurationId, RateLimiter<HttpExchange> rateLimiter) {
        Preconditions.checkNotNull(configurationId, "we cannot register a rateLimiter for a 'null' configurationId");
        Preconditions.checkNotNull(rateLimiter, "we cannot register a 'null' rate limiter for the configurationId " + configurationId);
        LOGGER.info("registration of a shared rateLimiter for the configurationId '{}'", configurationId);
        sharedRateLimiters.put(configurationId, rateLimiter);
    }
}
