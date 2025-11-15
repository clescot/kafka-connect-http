package io.github.clescot.kafka.connect.http.client.config;

import io.github.clescot.kafka.connect.http.core.HttpRequest;

import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Build a predicate for an HttpRequest from a map of configuration.
 * The predicate is used to match a request with a configuration.
 */
public class HttpRequestPredicateBuilder {
    //predicate
    public static final String PREDICATE = "predicate.";
    public static final String URL_REGEX = PREDICATE + "url.regex";
    public static final String METHOD_REGEX = PREDICATE + "method.regex";
    public static final String BODYTYPE_REGEX = PREDICATE + "bodytype.regex";
    public static final String HEADER_KEY_REGEX = PREDICATE + "header.key.regex";
    public static final String HEADER_VALUE_REGEX = PREDICATE + "header.value.regex";

    //tests only
    protected HttpRequestPredicateBuilder() {}

    public static HttpRequestPredicateBuilder build(){
        return new HttpRequestPredicateBuilder();
    }

    public Predicate<HttpRequest> buildPredicate(Map<String, String> configMap) {
        Predicate<HttpRequest> predicate = httpRequest -> true;
        if (configMap.containsKey(URL_REGEX)) {
            String urlRegex = configMap.get(URL_REGEX);
            Pattern urlPattern = Pattern.compile(urlRegex);
            predicate = predicate.and(httpRequest -> urlPattern.matcher(httpRequest.getUrl()).matches());
        }
        if (configMap.containsKey(METHOD_REGEX)) {
            String methodRegex = configMap.get(METHOD_REGEX);
            Pattern methodPattern = Pattern.compile(methodRegex);
            predicate = predicate.and(httpRequest -> methodPattern.matcher(httpRequest.getMethod().name()).matches());
        }
        if (configMap.containsKey(BODYTYPE_REGEX)) {
            String bodytypeRegex = configMap.get(BODYTYPE_REGEX);
            Pattern bodytypePattern = Pattern.compile(bodytypeRegex);
            predicate = predicate.and(httpRequest -> bodytypePattern.matcher(httpRequest.getBodyType().name()).matches());
        }
        if (configMap.containsKey(HEADER_KEY_REGEX)) {
            String headerKeyRegex = configMap.get(HEADER_KEY_REGEX);
            Pattern headerKeyPattern = Pattern.compile(headerKeyRegex);
            Predicate<HttpRequest> headerKeyPredicate = httpRequest -> httpRequest
                    .getHeaders()
                    .entrySet()
                    .stream()
                    .anyMatch(entry -> {
                        boolean headerKeyFound = headerKeyPattern.matcher(entry.getKey()).matches();
                        if (headerKeyFound
                                && entry.getValue() != null
                                && !entry.getValue().isEmpty()
                                && configMap.containsKey(HEADER_VALUE_REGEX)) {
                            String headerValue = configMap.get(HEADER_VALUE_REGEX);
                            Pattern headerValuePattern = Pattern.compile(headerValue);
                            return headerValuePattern.matcher(entry.getValue().get(0)).matches();
                        } else {
                            return headerKeyFound;
                        }

                    });
            predicate = predicate.and(headerKeyPredicate);
        }
        return predicate;
    }
}
