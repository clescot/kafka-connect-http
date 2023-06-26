package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.sink.client.HttpClient;
import io.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.sink.client.HttpException;
import io.github.clescot.kafka.connect.http.sink.client.ahc.AHCHttpClientFactory;
import io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.sink.config.AddStaticHeadersToHttpRequestFunction;
import io.github.clescot.kafka.connect.http.sink.config.AddSuccessStatusToHttpExchangeFunction;
import io.github.clescot.kafka.connect.http.sink.config.AddTrackingHeadersToHttpRequestFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

/**
 * Configuration of the http call mechanism, specific to some websites according to the configured <span class="strong">predicate</span>.
 * <p>
 * It permits to customize :
 * <ul>
 * <li>a success http response code regex</li>
 * <li>a retry http response code regex</li>
 * <li>a custom rate limiter</li>
 * </ul>
 */
public class Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);


    //predicate
    public static final String PREDICATE = "predicate.";
    public static final String URL_REGEX = PREDICATE+"url.regex";
    public static final String METHOD_REGEX = PREDICATE+"method.regex";
    public static final String BODYTYPE_REGEX = PREDICATE+"bodytype.regex";
    public static final String HEADER_KEY = PREDICATE+"header.key";
    public static final String HEADER_VALUE = PREDICATE+"header.value";


    private Predicate<HttpRequest> mainpredicate = httpRequest -> true;


    public static final String STATIC_SCOPE = "static";

    //enrich
    private final Pattern defaultSuccessPattern = Pattern.compile(CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);


    private final AddStaticHeadersToHttpRequestFunction addStaticHeadersToHttpRequestFunction;
    private final AddTrackingHeadersToHttpRequestFunction addTrackingHeadersToHttpRequestFunction;
    private AddSuccessStatusToHttpExchangeFunction addSuccessStatusToHttpExchangeFunction;

    //rate limiter
    private static final Map<String, RateLimiter<HttpExchange>> sharedRateLimiters = Maps.newHashMap();
    private RateLimiter<HttpExchange> rateLimiter;
    private Pattern retryResponseCodeRegex;


    //retry policy
    private RetryPolicy<HttpExchange> retryPolicy;

    //http client
    private HttpClient httpClient;
    public final String id;

    public Configuration(String id, HttpSinkConnectorConfig httpSinkConnectorConfig, ExecutorService executorService) {
        this.id = id;
        Preconditions.checkNotNull(id, "id must not be null");
        Preconditions.checkNotNull(httpSinkConnectorConfig, "httpSinkConnectorConfig must not be null");

        //configuration id prefix is not present into the configMap
        Map<String, Object> configMap = httpSinkConnectorConfig.originalsWithPrefix("config." + id + ".");

        //build addStaticHeadersFunction
        Optional<String> staticHeaderParam = Optional.ofNullable((String) configMap.get(STATIC_REQUEST_HEADER_NAMES));
        Map<String,List<String>> staticRequestHeaders = Maps.newHashMap();
        if (staticHeaderParam.isPresent()) {
            List<String> staticRequestHeaderNames = Arrays.asList(staticHeaderParam.get().split(","));
            for (String headerName : staticRequestHeaderNames) {
                String value = (String) configMap.get(STATIC_REQUEST_HEADER_NAMES+headerName);
                Preconditions.checkNotNull(value, "'" + headerName + "' is not configured as a parameter.");
                staticRequestHeaders.put(headerName, Lists.newArrayList(value));
            }
        }
        this.addStaticHeadersToHttpRequestFunction = new AddStaticHeadersToHttpRequestFunction(staticRequestHeaders);

        //build addTrackingHeadersFunction
        boolean generateMissingRequestId = (boolean) Optional.ofNullable(configMap.get(GENERATE_MISSING_REQUEST_ID)).orElse(false);
        boolean generateMissingCorrelationId = (boolean) Optional.ofNullable(configMap.get(GENERATE_MISSING_CORRELATION_ID)).orElse(false);
        this.addTrackingHeadersToHttpRequestFunction = new AddTrackingHeadersToHttpRequestFunction(generateMissingRequestId,generateMissingCorrelationId);

        this.httpClient = buildHttpClient(configMap, executorService);

        //main predicate
        if (configMap.containsKey(URL_REGEX)) {
            String urlRegex = (String) configMap.get(URL_REGEX);
            Pattern urlPattern = Pattern.compile(urlRegex);
            mainpredicate = mainpredicate.and(httpRequest -> urlPattern.matcher(httpRequest.getUrl()).matches());
        }
        if (configMap.containsKey(METHOD_REGEX)) {
            String methodRegex = (String) configMap.get(METHOD_REGEX);
            Pattern methodPattern = Pattern.compile(methodRegex);
            mainpredicate = mainpredicate.and(httpRequest -> methodPattern.matcher(httpRequest.getMethod()).matches());
        }
        if (configMap.containsKey(BODYTYPE_REGEX)) {
            String bodytypeRegex = (String) configMap.get(BODYTYPE_REGEX);
            Pattern bodytypePattern = Pattern.compile(bodytypeRegex);
            mainpredicate = mainpredicate.and(httpRequest -> bodytypePattern.matcher(httpRequest.getBodyType().name()).matches());
        }
        if (configMap.containsKey(HEADER_KEY)) {
            String headerKey = (String) configMap.get(HEADER_KEY);

            Predicate<HttpRequest> headerKeyPredicate = httpRequest -> httpRequest.getHeaders().containsKey(headerKey);
            mainpredicate = mainpredicate.and(headerKeyPredicate);
            if (configMap.containsKey(HEADER_VALUE)) {
                String headerValue = (String) configMap.get(HEADER_VALUE);
                Pattern headerValuePattern = Pattern.compile(headerValue);
                mainpredicate = mainpredicate.and(httpRequest -> headerValuePattern.matcher(httpRequest.getHeaders().get(headerKey).get(0)).matches());
            }

        }

        //rate limiter
        if (configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)) {
            long maxExecutions = Long.parseLong((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            long periodInMs = Long.parseLong(Optional.ofNullable((String) configMap.get(RATE_LIMITER_PERIOD_IN_MS)).orElse(httpSinkConnectorConfig.getDefaultRateLimiterPeriodInMs() + ""));
            if (configMap.containsKey(RATE_LIMITER_SCOPE) && STATIC_SCOPE.equalsIgnoreCase((String) configMap.get(RATE_LIMITER_SCOPE))) {
                Optional<RateLimiter<HttpExchange>> sharedRateLimiter = Optional.ofNullable(sharedRateLimiters.get(id));
                if (sharedRateLimiter.isPresent()) {
                    this.rateLimiter = sharedRateLimiter.get();
                } else {
                    RateLimiter<HttpExchange> myRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
                    registerRateLimiter(id, myRateLimiter);
                    this.rateLimiter = myRateLimiter;
                }
            } else {
                this.rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
            }
        }

        //success response code regex
        Pattern successResponseCodeRegex;
        if (configMap.containsKey(SUCCESS_RESPONSE_CODE_REGEX)) {
            successResponseCodeRegex = Pattern.compile((String) configMap.get(SUCCESS_RESPONSE_CODE_REGEX));
        }else{
            successResponseCodeRegex = defaultSuccessPattern;
        }
        this.addSuccessStatusToHttpExchangeFunction = new AddSuccessStatusToHttpExchangeFunction(successResponseCodeRegex);


        //retry response code regex
        if (configMap.containsKey(RETRY_RESPONSE_CODE_REGEX)) {
            this.retryResponseCodeRegex = Pattern.compile((String) configMap.get(RETRY_RESPONSE_CODE_REGEX));
        }

        //retry policy
        if (configMap.containsKey(RETRIES)) {
            Integer retries = Integer.parseInt((String) configMap.get(RETRIES));
            Long retryDelayInMs = Long.parseLong((String) configMap.get(RETRY_DELAY_IN_MS));
            Long retryMaxDelayInMs = Long.parseLong((String) configMap.get(RETRY_MAX_DELAY_IN_MS));
            Double retryDelayFactor = Double.parseDouble((String) configMap.get(RETRY_DELAY_FACTOR));
            Long retryJitterInMs = Long.parseLong((String) configMap.get(RETRY_JITTER_IN_MS));
            this.retryPolicy = buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitterInMs);
        }

    }

    public HttpRequest enrich(HttpRequest httpRequest) {
        return addStaticHeadersToHttpRequestFunction
                .andThen(addTrackingHeadersToHttpRequestFunction)
                .apply(httpRequest);
    }


    public HttpExchange enrich(HttpExchange httpExchange) {
        return this.addSuccessStatusToHttpExchangeFunction.apply(httpExchange);
    }
    private HttpClient buildHttpClient(Map<String, Object> config, ExecutorService executorService) {

        Class<? extends HttpClientFactory> httpClientFactoryClass;
        String httpClientImplementation = (String) Optional.ofNullable(config.get(HTTPCLIENT_IMPLEMENTATION)).orElse(OKHTTP_IMPLEMENTATION);
        if (AHC_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)) {
            httpClientFactoryClass = AHCHttpClientFactory.class;
        } else if (OKHTTP_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)) {
            httpClientFactoryClass = OkHttpClientFactory.class;
        } else {
            LOGGER.error("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '{}'", httpClientImplementation);
            throw new IllegalArgumentException("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '" + httpClientImplementation + "'");
        }
        HttpClientFactory httpClientFactory;
        try {
            httpClientFactoryClass = (Class<HttpClientFactory>) Class.forName(httpClientFactoryClass.getName());
            httpClientFactory = httpClientFactoryClass.getDeclaredConstructor().newInstance();
            LOGGER.debug("using HttpClientFactory implementation: {}", httpClientFactory.getClass().getName());
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return httpClientFactory.build(config, executorService);
    }


    public HttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setRateLimiter(RateLimiter<HttpExchange> rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public void setSuccessResponseCodeRegex(Pattern successResponseCodeRegex) {
        this.addSuccessStatusToHttpExchangeFunction =new AddSuccessStatusToHttpExchangeFunction(successResponseCodeRegex);
    }

    public void setRetryResponseCodeRegex(Pattern retryResponseCodeRegex) {
        this.retryResponseCodeRegex = retryResponseCodeRegex;
    }

    public void setRetryPolicy(RetryPolicy<HttpExchange> retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public static void registerRateLimiter(String configurationId, RateLimiter<HttpExchange> rateLimiter) {
        Preconditions.checkNotNull(configurationId, "we cannot register a rateLimiter for a 'null' configurationId");
        Preconditions.checkNotNull(rateLimiter, "we cannot register a 'null' rate limiter for the configurationId " + configurationId);
        LOGGER.info("registration of a shared rateLimiter for the configurationId '{}'", configurationId);
        sharedRateLimiters.put(configurationId, rateLimiter);
    }

    public Optional<RateLimiter<HttpExchange>> getRateLimiter() {
        return Optional.ofNullable(rateLimiter);
    }

    public Optional<RetryPolicy<HttpExchange>> getRetryPolicy() {
        return Optional.ofNullable(retryPolicy);
    }

    public Pattern getSuccessResponseCodeRegex() {
        return addSuccessStatusToHttpExchangeFunction.getSuccessResponseCodeRegex();
    }

    public Optional<Pattern> getRetryResponseCodeRegex() {
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

    public boolean matches(HttpRequest httpRequest) {
        return this.mainpredicate.test(httpRequest);
    }



    public String getId() {
        return id;
    }





    public AddStaticHeadersToHttpRequestFunction getAddStaticHeadersFunction() {
        return addStaticHeadersToHttpRequestFunction;
    }

    public AddTrackingHeadersToHttpRequestFunction getAddTrackingHeadersFunction() {
        return addTrackingHeadersToHttpRequestFunction;
    }
}
