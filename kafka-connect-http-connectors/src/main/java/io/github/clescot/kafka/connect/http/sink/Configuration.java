package io.github.clescot.kafka.connect.http.sink;

import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.sink.client.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);
    public static final String URL_REGEX = "url.regex";
    public static final String METHOD_REGEX = "method.regex";
    public static final String BODYTYPE_REGEX = "bodytype.regex";
    public static final String HEADER_KEY = "header.key";
    public static final String HEADER_VALUE = "header.value";
    private String retryResponseCodeRegex;
    private RateLimiter<HttpExchange> rateLimiter;
    private RetryPolicy<HttpExchange> retryPolicy;
    private Predicate<HttpRequest> mainpredicate = httpRequest -> true;
    public Configuration(String id,HttpSinkConnectorConfig httpSinkConnectorConfig) {
        Map<String, Object> configMap = httpSinkConnectorConfig.originalsWithPrefix("httpclient." + id+".");

        //main predicate
        if(configMap.containsKey(URL_REGEX)){
            String urlRegex = (String) configMap.get(URL_REGEX);
            Pattern urlPattern = Pattern.compile(urlRegex);
            mainpredicate = mainpredicate.and(httpRequest -> urlPattern.matcher(httpRequest.getUrl()).matches());
        }
        if(configMap.containsKey(METHOD_REGEX)){
            String methodRegex = (String) configMap.get(METHOD_REGEX);
            Pattern methodPattern = Pattern.compile(methodRegex);
            mainpredicate = mainpredicate.and(httpRequest -> methodPattern.matcher(httpRequest.getMethod()).matches());
        }
        if(configMap.containsKey(BODYTYPE_REGEX)){
            String bodytypeRegex = (String) configMap.get(BODYTYPE_REGEX);
            Pattern bodytypePattern = Pattern.compile(bodytypeRegex);
            mainpredicate = mainpredicate.and(httpRequest -> bodytypePattern.matcher(httpRequest.getBodyType().name()).matches());
        }
        if(configMap.containsKey(HEADER_KEY)){
            String headerKey = (String) configMap.get(HEADER_KEY);

            Predicate<HttpRequest> headerKeyPredicate = httpRequest -> httpRequest.getHeaders().containsKey(headerKey);
            mainpredicate = mainpredicate.and(headerKeyPredicate);
            if(configMap.containsKey(HEADER_VALUE)){
                String headerValue = (String) configMap.get(HEADER_VALUE);
                Pattern headerValuePattern = Pattern.compile(headerValue);
                mainpredicate = mainpredicate.and(httpRequest -> headerValuePattern.matcher(httpRequest.getHeaders().get(headerKey).get(0)).matches());
            }

        }

        //rate limiter
        if(configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)){
            long maxExecutions = Long.parseLong((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            long periodInMs = Long.parseLong(Optional.ofNullable((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS)).orElse(httpSinkConnectorConfig.getDefaultRateLimiterPeriodInMs()+""));
            this.rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
        }

        //retry response code regex
        if(configMap.containsKey(RETRY_RESPONSE_CODE_REGEX)){
            this.retryResponseCodeRegex = (String) configMap.get(RETRY_RESPONSE_CODE_REGEX);
        }

        //retry policy
        if(configMap.containsKey(RETRIES)) {
            Integer retries = Integer.parseInt((String) configMap.get(RETRIES));
            Long retryDelayInMs = Long.parseLong((String) configMap.get(RETRY_DELAY_IN_MS));
            Long retryMaxDelayInMs = Long.parseLong((String) configMap.get(RETRY_MAX_DELAY_IN_MS));
            Double retryDelayFactor = Double.parseDouble((String) configMap.get(RETRY_DELAY_FACTOR));
            Long retryJitterInMs = Long.parseLong((String) configMap.get(RETRY_JITTER_IN_MS));
            this.retryPolicy = buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitterInMs);
        }

    }

    public Optional<RateLimiter<HttpExchange>> getRateLimiter() {
        return Optional.ofNullable(rateLimiter);
    }

    public Optional<RetryPolicy<HttpExchange>> getRetryPolicy() {
        return Optional.ofNullable(retryPolicy);
    }

    public Optional<String> getRetryResponseCodeRegex(){
        return Optional.ofNullable(retryResponseCodeRegex);
    }

    private RetryPolicy<HttpExchange> buildRetryPolicy(Integer retries,
                                                       Long retryDelayInMs,
                                                       Long retryMaxDelayInMs,
                                                       Double retryDelayFactor,
                                                       Long retryJitterInMs) {
        return RetryPolicy.<HttpExchange>builder()
                //we retry only if the error comes from the WS server (server-side technical error)
                .handle(HttpException.class)
                .withBackoff(Duration.ofMillis(retryDelayInMs), Duration.ofMillis(retryMaxDelayInMs), retryDelayFactor)
                .withJitter(Duration.ofMillis(retryJitterInMs))
                .withMaxRetries(retries)
                .onRetry(listener -> LOGGER.warn("Retry ws call result:{}, failure:{}", listener.getLastResult(), listener.getLastException()))
                .onFailure(listener -> LOGGER.warn("ws call failed ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .onAbort(listener -> LOGGER.warn("ws call aborted ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .build();
    }

    public boolean matches(HttpRequest httpRequest){
        return this.mainpredicate.test(httpRequest);
    }
}
