package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.VersionUtils;
import io.github.clescot.kafka.connect.http.client.config.*;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

/**
 * Configuration of the {@link HttpClient}, specific to some websites according to the configured <span class="strong">predicate</span>.
 * @param <C> client type, which is a subclass of HttpClient
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 * <p>
 * It permits to customize :
 * <ul>
 * <li>a success http response code regex</li>
 * <li>a retry http response code regex</li>
 * <li>a custom rate limiter</li>
 * </ul>
 * Each configuration owns an Http Client instance.
 */
public class HttpClientConfiguration<C extends HttpClient<R,S>,R,S> implements Configuration<C,HttpRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientConfiguration.class);
    public static final String HAS_BEEN_SET = " has been set.";
    public static final String SHA_1_PRNG = "SHA1PRNG";
    public static final String MUST_BE_SET_TOO = " must be set too.";
    public static final String CONFIGURATION_ID = "configuration.id";
    public static final String USER_AGENT_HTTP_CLIENT_DEFAULT_MODE = "http_client";
    public static final String USER_AGENT_PROJECT_MODE = "project";
    public static final String USER_AGENT_CUSTOM_MODE = "custom";

    private final Predicate<HttpRequest> predicate;


    public static final String STATIC_SCOPE = "static";

    //enrich
    private final Pattern defaultSuccessPattern = Pattern.compile(CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);


    private final AddStaticHeadersToHttpRequestFunction addStaticHeadersToHttpRequestFunction;
    private final AddMissingRequestIdHeaderToHttpRequestFunction addMissingRequestIdHeaderToHttpRequestFunction;
    private final AddMissingCorrelationIdHeaderToHttpRequestFunction addMissingCorrelationIdHeaderToHttpRequestFunction;
    private AddSuccessStatusToHttpExchangeFunction addSuccessStatusToHttpExchangeFunction;
    private AddUserAgentHeaderToHttpRequestFunction addUserAgentHeaderToHttpRequestFunction;

    //retry policy
    private Pattern retryResponseCodeRegex;
    private RetryPolicy<HttpExchange> retryPolicy;

    //http client
    private C httpClient;
    public final String id;
    private final ExecutorService executorService;
    private final Map<String, Object> settings;
    private final Function<HttpRequest, HttpRequest> enrichRequestFunction;
    public HttpClientConfiguration(String id,
                                   HttpClientFactory<C,R,S> httpClientFactory,
                                   Map<String,String> config,
                                   ExecutorService executorService,
                                   CompositeMeterRegistry meterRegistry) {
        this.id = id;
        this.executorService = executorService;
        Preconditions.checkNotNull(id, "id must not be null");
        Preconditions.checkNotNull(config, "httpSinkConnectorConfig must not be null");
        Preconditions.checkNotNull(httpClientFactory,"httpClientFactory must not be null");

        //configuration id prefix is not present in the resulting configMap
        this.settings = Maps.newHashMap(config);
        settings.put(CONFIGURATION_ID, id);
        //main predicate
        this.predicate = HttpRequestPredicateBuilder.build().buildPredicate(settings);

        Random random = getRandom(settings);

        this.httpClient = httpClientFactory.buildHttpClient(settings, executorService, meterRegistry, random);

        //enrich request
        List<Function<HttpRequest,HttpRequest>> enrichRequestFunctions = Lists.newArrayList();
        //build addStaticHeadersFunction
        Optional<String> staticHeaderParam = Optional.ofNullable((String) settings.get(STATIC_REQUEST_HEADER_NAMES));
        Map<String, List<String>> staticRequestHeaders = Maps.newHashMap();
        if (staticHeaderParam.isPresent()) {
            String[] staticRequestHeaderNames = staticHeaderParam.get().split(",");
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
        String activateUserAgentHeaderToHttpRequestFunction = (String) settings.getOrDefault(USER_AGENT_OVERRIDE, USER_AGENT_HTTP_CLIENT_DEFAULT_MODE);
        if (USER_AGENT_HTTP_CLIENT_DEFAULT_MODE.equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)) {
            LOGGER.trace("userAgentHeaderToHttpRequestFunction : 'http_client' configured. No need to activate UserAgentInterceptor");
        }else if(USER_AGENT_PROJECT_MODE.equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)){
            VersionUtils versionUtils = new VersionUtils();
            String projectUserAgent = "Mozilla/5.0 (compatible;kafka-connect-http/"+ versionUtils.getVersion() +"; "+httpClient.getEngineId()+"; https://github.com/clescot/kafka-connect-http)";
            this.addUserAgentHeaderToHttpRequestFunction = new AddUserAgentHeaderToHttpRequestFunction(Lists.newArrayList(projectUserAgent), random);
            enrichRequestFunctions.add(addUserAgentHeaderToHttpRequestFunction);
        }else if(USER_AGENT_CUSTOM_MODE.equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)){
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


        //retry policy
        //retry response code regex
        if (settings.containsKey(RETRY_RESPONSE_CODE_REGEX)) {
            this.retryResponseCodeRegex = Pattern.compile((String) settings.get(RETRY_RESPONSE_CODE_REGEX));
        }

        if (settings.containsKey(RETRIES)) {
            Integer retries = Integer.parseInt((String) settings.get(RETRIES));
            Long retryDelayInMs = Long.parseLong((String) Optional.ofNullable(settings.get(RETRY_DELAY_IN_MS)).orElse(""+DEFAULT_RETRY_DELAY_IN_MS_VALUE));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_DELAY_IN_MS + MUST_BE_SET_TOO);
            Long retryMaxDelayInMs = Long.parseLong((String) Optional.ofNullable(settings.get(RETRY_MAX_DELAY_IN_MS)).orElse(""+DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_MAX_DELAY_IN_MS + MUST_BE_SET_TOO);
            Double retryDelayFactor = Double.parseDouble((String) Optional.ofNullable(settings.get(RETRY_DELAY_FACTOR)).orElse(""+DEFAULT_RETRY_DELAY_FACTOR_VALUE));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_DELAY_FACTOR + MUST_BE_SET_TOO);
            Long retryJitterInMs = Long.parseLong((String) Optional.ofNullable(settings.get(RETRY_JITTER_IN_MS)).orElse(""+DEFAULT_RETRY_JITTER_IN_MS_VALUE));
            Preconditions.checkNotNull(retryDelayInMs, RETRIES + HAS_BEEN_SET + RETRY_JITTER_IN_MS + MUST_BE_SET_TOO);
            this.retryPolicy = buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitterInMs);
        }else{
            LOGGER.trace("configuration '{}' :retry policy is not configured",this.getId());
        }

    }

    public Function<HttpRequest, HttpRequest> getEnrichRequestFunction() {
        return enrichRequestFunction;
    }

    public AddSuccessStatusToHttpExchangeFunction getAddSuccessStatusToHttpExchangeFunction() {
        return addSuccessStatusToHttpExchangeFunction;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    @java.lang.SuppressWarnings({"java:S2119","java:S2245"})
    @NotNull
    private Random getRandom(Map<String, Object> config) {
        Random random;

        try {
        if(config.containsKey(HTTP_CLIENT_SECURE_RANDOM_ACTIVATE)&&(boolean)config.get(HTTP_CLIENT_SECURE_RANDOM_ACTIVATE)){
            String rngAlgorithm = SHA_1_PRNG;
            if (config.containsKey(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM)) {
                rngAlgorithm = (String) config.get(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM);
            }
            random = SecureRandom.getInstance(rngAlgorithm);
        }else {
            if(config.containsKey(HTTP_CLIENT_UNSECURE_RANDOM_SEED)){
                long seed = (long) config.get(HTTP_CLIENT_UNSECURE_RANDOM_SEED);
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

    @Override
    public C getClient() {
        return httpClient;
    }

    public void setHttpClient(C httpClient) {
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
        //noinspection LoggingPlaceholderCountMatchesArgumentCount
        return RetryPolicy.<HttpExchange>builder()
                //we retry only if the error comes from the WS server (server-side technical error)
                .handle(HttpException.class)
                .withBackoff(Duration.ofMillis(retryDelayInMs), Duration.ofMillis(retryMaxDelayInMs), retryDelayFactor)
                .withJitter(Duration.ofMillis(retryJitterInMs))
                .withMaxRetries(retries)
                .onAbort(listener -> LOGGER.warn("Retry  aborted after elapsed attempt time:'{}' attempts:'{}',result:'{}', failure:'{}'", listener.getElapsedAttemptTime(), listener.getAttemptCount(), listener.getResult(), listener.getException()))
                .onRetriesExceeded(listener -> LOGGER.warn("Retries exceeded  elapsed attempt time:'{}', attempts:'{}', call result:'{}', failure:'{}'",listener.getElapsedAttemptTime(), listener.getAttemptCount(),listener.getResult(), listener.getException()))
                .onRetry(listener -> LOGGER.trace("Retry  call result:'{}', failure:'{}'", listener.getLastResult(), listener.getLastException()))
                .onFailure(listener -> LOGGER.warn("call failed ! result:'{}',exception:'{}'", listener.getResult(), listener.getException()))
                .onAbort(listener -> LOGGER.warn("call aborted ! result:'{}',exception:'{}'", listener.getResult(), listener.getException()))
                .build();
    }

    public boolean matches(HttpRequest httpRequest) {
        return this.predicate.test(httpRequest);
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
                "', predicate='" + predicateToString() +
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
