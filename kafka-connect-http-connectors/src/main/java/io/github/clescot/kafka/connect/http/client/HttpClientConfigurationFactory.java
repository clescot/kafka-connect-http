package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.jetbrains.annotations.NotNull;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.HTTP_CLIENT_UNSECURE_RANDOM_SEED;

public class HttpClientConfigurationFactory {
    public static final String SHA_1_PRNG = "SHA1PRNG";

    private HttpClientConfigurationFactory() {
    }

    public static  <C extends HttpClient<R, S>, R, S>Map<String,HttpClientConfiguration<C, R, S>> buildConfigurations(
            HttpClientFactory<C, R, S> httpClientFactory,
            ExecutorService executorService,
            List<String> configIdList,
            Map<String, String> originals, CompositeMeterRegistry meterRegistry
    ) {
        Map<String,HttpClientConfiguration<C, R, S>> httpClientConfigurations = Maps.newHashMap();
        List<String> configurationIds = Lists.newArrayList();
        Optional<List<String>> ids = Optional.ofNullable(configIdList);
        ids.ifPresent(configurationIds::addAll);
        HttpClientConfiguration<C, R, S> defaultHttpClientConfiguration = null;
        Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();
        Optional<Pattern> defaultRetryResponseCodeRegex = Optional.empty();
        for (String configId : configurationIds) {
            Map<String, String> config = Maps.newHashMap(MapUtils.getMapWithPrefix(originals, "config." + configId + "."));
            HashMap<String, String> settings = Maps.newHashMap(config);
            settings.put("configuration.id", configId);
            Random random = getRandom(settings);
            C httpClient = httpClientFactory.buildHttpClient(settings, executorService, meterRegistry, random);
            HttpClientConfiguration<C, R, S> httpClientConfiguration = new HttpClientConfiguration<>(configId,config,httpClient);
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

    @java.lang.SuppressWarnings({"java:S2119","java:S2245"})
    @NotNull
    public static Random getRandom(Map<String, String> config) {
        Random random;

        try {
            if(config.containsKey(HTTP_CLIENT_SECURE_RANDOM_ACTIVATE)&&Boolean.parseBoolean(config.get(HTTP_CLIENT_SECURE_RANDOM_ACTIVATE))){
                String rngAlgorithm = SHA_1_PRNG;
                if (config.containsKey(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM)) {
                    rngAlgorithm = config.get(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM);
                }
                random = SecureRandom.getInstance(rngAlgorithm);
            }else {
                if(config.containsKey(HTTP_CLIENT_UNSECURE_RANDOM_SEED)){
                    long seed = Long.parseLong(config.get(HTTP_CLIENT_UNSECURE_RANDOM_SEED));
                    random = new Random(seed);
                }else {
                    random = new Random();
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new HttpException(e);
        }
        return random;
    }

}
