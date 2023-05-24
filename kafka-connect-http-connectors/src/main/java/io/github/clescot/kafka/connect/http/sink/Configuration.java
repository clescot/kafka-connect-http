package io.github.clescot.kafka.connect.http.sink;

import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.RATE_LIMITER_MAX_EXECUTIONS;

public class Configuration {
    public static final String URL_REGEX = "url.regex";
    public static final String METHOD_REGEX = "method.regex";
    public static final String BODYTYPE_REGEX = "bodytype.regex";
    public static final String HEADER_KEY = "header.key";
    public static final String HEADER_VALUE = "header.value";
    private RateLimiter<HttpExchange> rateLimiter;
    private Predicate<HttpRequest> mainpredicate = httpRequest -> true;
    public Configuration(String id,HttpSinkConnectorConfig httpSinkConnectorConfig) {
        Map<String, Object> configMap = httpSinkConnectorConfig.originalsWithPrefix("httpclient." + id+".");

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
        if(configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)){
            long maxExecutions = Long.parseLong((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            long periodInMs = Long.parseLong(Optional.ofNullable((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS)).orElse(httpSinkConnectorConfig.getDefaultRateLimiterPeriodInMs()+""));
            this.rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();

        }
    }

    public Optional<RateLimiter<HttpExchange>> getRateLimiter() {
        return Optional.ofNullable(rateLimiter);
    }

    public boolean matches(HttpRequest httpRequest){
        return this.mainpredicate.test(httpRequest);
    }
}
