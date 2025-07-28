package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;

public class HttpClientConfigurationFactory {


    private HttpClientConfigurationFactory() {
    }

    public static  <C extends HttpClient<R, S>, R, S>Map<String,HttpClientConfiguration<C, R, S>> buildConfigurations(
            HttpClientFactory<C, R, S> httpClientFactory,
            ExecutorService executorService,
            List<String> configIdList,
            Map<String, Object> originals, CompositeMeterRegistry meterRegistry
    ) {
        Map<String,HttpClientConfiguration<C, R, S>> httpClientConfigurations = Maps.newHashMap();
        List<String> configurationIds = Lists.newArrayList();
        Optional<List<String>> ids = Optional.ofNullable(configIdList);
        configurationIds.add(DEFAULT_CONFIGURATION_ID);
        ids.ifPresent(configurationIds::addAll);
        HttpClientConfiguration<C, R, S> defaultHttpClientConfiguration = null;
        Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();
        Optional<Pattern> defaultRetryResponseCodeRegex = Optional.empty();
        for (String configId : configurationIds) {

            HttpClientConfiguration<C, R, S> httpClientConfiguration = new HttpClientConfiguration<>(configId, httpClientFactory,  MapUtils.getMapWithPrefix(originals,"config." + configId + "."), executorService, meterRegistry);
            if (httpClientConfiguration.getClient() == null && !httpClientConfigurations.isEmpty() && defaultHttpClientConfiguration != null) {
                httpClientConfiguration.setHttpClient(defaultHttpClientConfiguration.getClient());
            }

            //we reuse the default retry policy if not set

            if (httpClientConfiguration.getRetryPolicy().isEmpty() && defaultRetryPolicy.isPresent()) {
                httpClientConfiguration.setRetryPolicy(defaultRetryPolicy.get());
            }
            //we reuse the default success response code regex if not set
            if (defaultHttpClientConfiguration != null) {
                httpClientConfiguration.setSuccessResponseCodeRegex(defaultHttpClientConfiguration.getSuccessResponseCodeRegex());
            }

            if (httpClientConfiguration.getRetryResponseCodeRegex().isEmpty() && defaultRetryResponseCodeRegex.isPresent()) {
                httpClientConfiguration.setRetryResponseCodeRegex(defaultRetryResponseCodeRegex.get());
            }
            if (DEFAULT_CONFIGURATION_ID.equals(configId)) {
                defaultHttpClientConfiguration = httpClientConfiguration;
                defaultRetryPolicy = defaultHttpClientConfiguration.getRetryPolicy();
                defaultRetryResponseCodeRegex = defaultHttpClientConfiguration.getRetryResponseCodeRegex();
            }
            httpClientConfigurations.put(configId,httpClientConfiguration);
        }
        return httpClientConfigurations;
    }

}
