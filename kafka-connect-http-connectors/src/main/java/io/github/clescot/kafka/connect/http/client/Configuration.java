package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.ahc.AHCHttpClientFactory;
import io.github.clescot.kafka.connect.http.client.config.*;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.client.proxy.ProxySelectorFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
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
 * Each configuration owns an Http Client instance.
 */
public class Configuration {
    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);


    //predicate
    public static final String PREDICATE = "predicate.";
    public static final String URL_REGEX = PREDICATE + "url.regex";
    public static final String METHOD_REGEX = PREDICATE + "method.regex";
    public static final String BODYTYPE_REGEX = PREDICATE + "bodytype.regex";
    public static final String HEADER_KEY_REGEX = PREDICATE + "header.key.regex";
    public static final String HEADER_VALUE_REGEX = PREDICATE + "header.value.regex";
    public static final String HAS_BEEN_SET = " has been set.";
    public static final String SHA_1_PRNG = "SHA1PRNG";
    public static final String MUST_BE_SET_TOO = " must be set too.";
    public static final String CONFIGURATION_ID = "configuration.id";

    private final Predicate<HttpRequest> mainpredicate;


    public static final String STATIC_SCOPE = "static";

    //enrich
    private final Pattern defaultSuccessPattern = Pattern.compile(CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);


    private final AddStaticHeadersToHttpRequestFunction addStaticHeadersToHttpRequestFunction;
    private final AddMissingRequestIdHeaderToHttpRequestFunction addMissingRequestIdHeaderToHttpRequestFunction;
    private final AddMissingCorrelationIdHeaderToHttpRequestFunction addMissingCorrelationIdHeaderToHttpRequestFunction;
    private AddSuccessStatusToHttpExchangeFunction addSuccessStatusToHttpExchangeFunction;
    private AddUserAgentHeaderToHttpRequestFunction addUserAgentHeaderToHttpRequestFunction;
    //rate limiter
    private static final Map<String, RateLimiter<HttpExchange>> sharedRateLimiters = Maps.newHashMap();


    //retry policy
    private Pattern retryResponseCodeRegex;
    private RetryPolicy<HttpExchange> retryPolicy;

    //http client
    private HttpClient httpClient;
    public final String id;
    private final Map<String, Object> settings;
    private Function<HttpRequest, HttpRequest> enrichRequestFunction;
    public Configuration(String id,
                         AbstractConfig config,
                         ExecutorService executorService,
                         CompositeMeterRegistry meterRegistry) {
        this.id = id;
        Preconditions.checkNotNull(id, "id must not be null");
        Preconditions.checkNotNull(config, "httpSinkConnectorConfig must not be null");

        //configuration id prefix is not present in the resulting configMap
        this.settings = config.originalsWithPrefix("config." + id + ".");
        settings.put(CONFIGURATION_ID, id);
        //main predicate
        this.mainpredicate = buildPredicate(settings);

        Random random = getRandom(settings);

        this.httpClient = buildHttpClient(settings, executorService, meterRegistry, random);

        //enrich request
        List<Function<HttpRequest,HttpRequest>> enrichRequestFunctions = Lists.newArrayList();
        //build addStaticHeadersFunction
        Optional<String> staticHeaderParam = Optional.ofNullable((String) settings.get(STATIC_REQUEST_HEADER_NAMES));
        Map<String, List<String>> staticRequestHeaders = Maps.newHashMap();
        if (staticHeaderParam.isPresent()) {
            List<String> staticRequestHeaderNames = Arrays.asList(staticHeaderParam.get().split(","));
            for (String headerName : staticRequestHeaderNames) {
                String value = (String) settings.get(STATIC_REQUEST_HEADER_PREFIX + headerName);
                Preconditions.checkNotNull(value, "'" + headerName + "' is not configured as a parameter.");
                ArrayList<String> values = Lists.newArrayList(value);
                staticRequestHeaders.put(headerName, values);
                LOGGER.debug("static header {}:{}", headerName, values);
            }
        }
        this.addStaticHeadersToHttpRequestFunction = new AddStaticHeadersToHttpRequestFunction(staticRequestHeaders);
        enrichRequestFunctions.add(addStaticHeadersToHttpRequestFunction);

        //AddMissingRequestIdHeaderToHttpRequestFunction
        boolean generateMissingRequestId = Boolean.parseBoolean((String) settings.get(GENERATE_MISSING_REQUEST_ID));
        this.addMissingRequestIdHeaderToHttpRequestFunction = new AddMissingRequestIdHeaderToHttpRequestFunction(generateMissingRequestId);
        enrichRequestFunctions.add(addMissingRequestIdHeaderToHttpRequestFunction);

        //AddMissingCorrelationIdHeaderToHttpRequestFunction
        boolean generateMissingCorrelationId = Boolean.parseBoolean((String) settings.get(GENERATE_MISSING_CORRELATION_ID));
        this.addMissingCorrelationIdHeaderToHttpRequestFunction = new AddMissingCorrelationIdHeaderToHttpRequestFunction(generateMissingCorrelationId);
        enrichRequestFunctions.add(addMissingCorrelationIdHeaderToHttpRequestFunction);

        //activateUserAgentHeaderToHttpRequestFunction
        String activateUserAgentHeaderToHttpRequestFunction = (String) settings.getOrDefault(USER_AGENT_OVERRIDE,"http_client");
        if ("http_client".equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)) {
            LOGGER.trace("userAgentHeaderToHttpRequestFunction : 'http_client' configured. No need to activate UserAgentInterceptor");
        }else if("project".equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)){
            VersionUtils versionUtils = new VersionUtils();
            String projectUserAgent = "Mozilla/5.0 (compatible;kafka-connect-http/"+ versionUtils.getVersion() +"; "+httpClient.getEngineId()+"; https://github.com/clescot/kafka-connect-http)";
            this.addUserAgentHeaderToHttpRequestFunction = new AddUserAgentHeaderToHttpRequestFunction(Lists.newArrayList(projectUserAgent), random);
            enrichRequestFunctions.add(addUserAgentHeaderToHttpRequestFunction);
        }else if("custom".equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)){
            String userAgentValuesAsString = settings.getOrDefault(USER_AGENT_CUSTOM_VALUES, StringUtils.EMPTY).toString();
            List<String> userAgentValues = Arrays.asList(userAgentValuesAsString.split("\\|"));
            this.addUserAgentHeaderToHttpRequestFunction = new AddUserAgentHeaderToHttpRequestFunction(userAgentValues, random);
            enrichRequestFunctions.add(addUserAgentHeaderToHttpRequestFunction);
        }else{
            LOGGER.trace("user agent interceptor : '{}' configured. No need to activate UserAgentInterceptor",activateUserAgentHeaderToHttpRequestFunction);
        }

        enrichRequestFunction = enrichRequestFunctions.stream().reduce(Function.identity(), Function::andThen);

        //enrich exchange
        //success response code regex
        Pattern successResponseCodeRegex;
        if (settings.containsKey(SUCCESS_RESPONSE_CODE_REGEX)) {
            successResponseCodeRegex = Pattern.compile((String) settings.get(SUCCESS_RESPONSE_CODE_REGEX));
        } else {
            successResponseCodeRegex = defaultSuccessPattern;
        }
        this.addSuccessStatusToHttpExchangeFunction = new AddSuccessStatusToHttpExchangeFunction(successResponseCodeRegex);





        //rate limiter
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        httpClient.setRateLimiter(buildRateLimiter(id, config, settings));


        //retry policy
        //retry response code regex
        if (settings.containsKey(RETRY_RESPONSE_CODE_REGEX)) {
            this.retryResponseCodeRegex = Pattern.compile((String) settings.get(RETRY_RESPONSE_CODE_REGEX));
        }

        if (settings.containsKey(RETRIES)) {
            Integer retries = Integer.parseInt((String) settings.get(RETRIES));
            Long retryDelayInMs = Long.parseLong((String) settings.get(RETRY_DELAY_IN_MS));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_DELAY_IN_MS + MUST_BE_SET_TOO);
            Long retryMaxDelayInMs = Long.parseLong((String) settings.get(RETRY_MAX_DELAY_IN_MS));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_MAX_DELAY_IN_MS + MUST_BE_SET_TOO);
            Double retryDelayFactor = Double.parseDouble((String) settings.get(RETRY_DELAY_FACTOR));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_DELAY_FACTOR + MUST_BE_SET_TOO);
            Long retryJitterInMs = Long.parseLong((String) settings.get(RETRY_JITTER_IN_MS));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_JITTER_IN_MS + MUST_BE_SET_TOO);
            this.retryPolicy = buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitterInMs);
        }

    }

    private RateLimiter<HttpExchange> buildRateLimiter(String id, AbstractConfig httpSinkConnectorConfig, Map<String, Object> configMap) {
        RateLimiter<HttpExchange> rateLimiter = null;
        if (configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)) {
            long maxExecutions = Long.parseLong((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            long defaultMaxExecutions = httpSinkConnectorConfig.getLong(CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS);
            long periodInMs = Long.parseLong(Optional.ofNullable((String) configMap.get(RATE_LIMITER_PERIOD_IN_MS)).orElse(defaultMaxExecutions + ""));
            if (configMap.containsKey(RATE_LIMITER_SCOPE) && STATIC_SCOPE.equalsIgnoreCase((String) configMap.get(RATE_LIMITER_SCOPE))) {
                Optional<RateLimiter<HttpExchange>> sharedRateLimiter = Optional.ofNullable(sharedRateLimiters.get(id));
                if (sharedRateLimiter.isPresent()) {
                    rateLimiter = sharedRateLimiter.get();
                } else {
                    RateLimiter<HttpExchange> myRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
                    registerRateLimiter(id, myRateLimiter);
                    rateLimiter = myRateLimiter;
                }
            } else {
                rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
            }
        }
        return rateLimiter;
    }

    private Predicate<HttpRequest> buildPredicate(Map<String, Object> configMap) {
        Predicate<HttpRequest> predicate = httpRequest -> true;
        if (configMap.containsKey(URL_REGEX)) {
            String urlRegex = (String) configMap.get(URL_REGEX);
            Pattern urlPattern = Pattern.compile(urlRegex);
            predicate = predicate.and(httpRequest -> urlPattern.matcher(httpRequest.getUrl()).matches());
        }
        if (configMap.containsKey(METHOD_REGEX)) {
            String methodRegex = (String) configMap.get(METHOD_REGEX);
            Pattern methodPattern = Pattern.compile(methodRegex);
            predicate = predicate.and(httpRequest -> methodPattern.matcher(httpRequest.getMethod()).matches());
        }
        if (configMap.containsKey(BODYTYPE_REGEX)) {
            String bodytypeRegex = (String) configMap.get(BODYTYPE_REGEX);
            Pattern bodytypePattern = Pattern.compile(bodytypeRegex);
            predicate = predicate.and(httpRequest -> bodytypePattern.matcher(httpRequest.getBodyType().name()).matches());
        }
        if (configMap.containsKey(HEADER_KEY_REGEX)) {
            String headerKeyRegex = (String) configMap.get(HEADER_KEY_REGEX);
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
                            String headerValue = (String) configMap.get(HEADER_VALUE_REGEX);
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

    public HttpRequest enrich(HttpRequest httpRequest) {
        return enrichRequestFunction.apply(httpRequest);
    }


    public HttpExchange enrich(HttpExchange httpExchange) {
        return this.addSuccessStatusToHttpExchangeFunction.apply(httpExchange);
    }

    private <Req, Res> HttpClient<Req, Res> buildHttpClient(Map<String, Object> config,
                                                            ExecutorService executorService,
                                                            CompositeMeterRegistry meterRegistry, Random random) {

        Class<? extends HttpClientFactory> httpClientFactoryClass;
        String httpClientImplementation = (String) Optional.ofNullable(config.get(CONFIG_HTTP_CLIENT_IMPLEMENTATION)).orElse(OKHTTP_IMPLEMENTATION);
        if (AHC_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)) {
            httpClientFactoryClass = AHCHttpClientFactory.class;
        } else if (OKHTTP_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)) {
            httpClientFactoryClass = OkHttpClientFactory.class;
        } else {
            LOGGER.error("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '{}'", httpClientImplementation);
            throw new IllegalArgumentException("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '" + httpClientImplementation + "'");
        }
        HttpClientFactory<Req, Res> httpClientFactory;
        try {
            httpClientFactory = httpClientFactoryClass.getDeclaredConstructor().newInstance();
            LOGGER.debug("using HttpClientFactory implementation: {}", httpClientFactory.getClass().getName());
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new HttpException(e);
        }


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
        return httpClientFactory.build(config, executorService, random, proxy, proxySelector, meterRegistry);
    }

    @NotNull
    private static Random getRandom(Map<String, Object> config) {
        Random random;
        String rngAlgorithm = SHA_1_PRNG;

        if (config.containsKey(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM)) {
            rngAlgorithm = (String) config.get(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM);
        }
        try {
            random = SecureRandom.getInstance(rngAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new HttpException(e);
        }
        return random;
    }


    public HttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public void setSuccessResponseCodeRegex(Pattern successResponseCodeRegex) {
        this.addSuccessStatusToHttpExchangeFunction = new AddSuccessStatusToHttpExchangeFunction(successResponseCodeRegex);
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

    public HttpExchange handleRetry(HttpExchange httpExchange) {
        //we don't retry success HTTP Exchange
        boolean responseCodeImpliesRetry = retryNeeded(httpExchange.getHttpResponse());
        LOGGER.debug("httpExchange success :'{}'", httpExchange.isSuccess());
        LOGGER.debug("response code('{}') implies retry:'{}'", httpExchange.getHttpResponse().getStatusCode(), responseCodeImpliesRetry);
        if (!httpExchange.isSuccess()
                && responseCodeImpliesRetry) {
            throw new HttpException(httpExchange, "retry needed");
        }
        return httpExchange;
    }


    protected boolean retryNeeded(HttpResponse httpResponse) {
        Optional<Pattern> myRetryResponseCodeRegex = getRetryResponseCodeRegex();
        if (myRetryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = myRetryResponseCodeRegex.get();
            Matcher matcher = retryPattern.matcher("" + httpResponse.getStatusCode());
            return matcher.matches();
        } else {
            return false;
        }
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

    public AddMissingRequestIdHeaderToHttpRequestFunction getAddTrackingHeadersFunction() {
        return addMissingRequestIdHeaderToHttpRequestFunction;
    }

    public AddUserAgentHeaderToHttpRequestFunction getAddUserAgentHeaderToHttpRequestFunction() {
        return addUserAgentHeaderToHttpRequestFunction;
    }

    private String predicateToString() {
        StringBuilder result = new StringBuilder("{");
        String urlRegex = (String) settings.get(URL_REGEX);
        if(urlRegex!=null) {
            result.append("urlRegex:'").append(urlRegex).append("'");
        }
        String methodRegex = (String) settings.get(METHOD_REGEX);
        if(methodRegex!=null) {
            result.append(",methodRegex:").append(methodRegex).append("'");
        }
        String bodytypeRegex = (String) settings.get(BODYTYPE_REGEX);
        if(bodytypeRegex!=null) {
            result.append(",bodytypeRegex:").append(bodytypeRegex).append("'");
        }
        String headerKeyRegex = (String) settings.get(HEADER_KEY_REGEX);
        if(headerKeyRegex!=null) {
            result.append(",headerKeyRegex:").append(headerKeyRegex).append("'");
        }
        result.append("}");
        return result.toString();
    }

    private String retryPolicyToString(){
        StringBuilder result = new StringBuilder("{");
        if(retryResponseCodeRegex!=null){
            result.append("retryResponseCodeRegex:'").append(retryResponseCodeRegex).append("'");
        }
        String retries = (String) settings.get(RETRIES);
        if(retries!=null){
            result.append(", retries:'").append(retries).append("'");
        }
        String retryDelayInMs = (String) settings.get(RETRY_DELAY_IN_MS);
        if(retryDelayInMs!=null){
            result.append(", retryDelayInMs:'").append(retryDelayInMs).append("'");
        }
        String maxRetryDelayInMs = (String) settings.get(RETRY_MAX_DELAY_IN_MS);
        if(maxRetryDelayInMs!=null){
            result.append(", maxRetryDelayInMs:'").append(maxRetryDelayInMs).append("'");
        }
        String retryDelayFactor = (String) settings.get(RETRY_DELAY_FACTOR);
        if(retryDelayFactor!=null){
            result.append(", retryDelayFactor:'").append(retryDelayFactor).append("'");
        }
        String retryjitterInMs = (String) settings.get(RETRY_JITTER_IN_MS);
        if(retryjitterInMs!=null){
            result.append(", retryjitterInMs:'").append(retryjitterInMs).append("'");
        }
        result.append("}");
        return result.toString();
    }
    @Override
    public String toString() {
        return "Configuration{" +
                "id='" + id +
                "', mainpredicate='" + predicateToString() +
                "', defaultSuccessPattern='" + defaultSuccessPattern +
                "', addStaticHeadersToHttpRequestFunction='" + addStaticHeadersToHttpRequestFunction +
                "', addMissingRequestIdHeaderToHttpRequestFunction='" + addMissingRequestIdHeaderToHttpRequestFunction +
                "', addMissingCorrelationIdHeaderToHttpRequestFunction='" + addMissingCorrelationIdHeaderToHttpRequestFunction +
                "', addSuccessStatusToHttpExchangeFunction='" + addSuccessStatusToHttpExchangeFunction +
                "', retryPolicy='" + retryPolicyToString() +
                "', httpClient='" + httpClient +
                '}';
    }
}
