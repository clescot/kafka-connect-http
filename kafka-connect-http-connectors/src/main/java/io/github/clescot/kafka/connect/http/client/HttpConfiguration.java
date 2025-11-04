package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import dev.failsafe.CircuitBreaker;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.http.client.config.AddSuccessStatusToHttpExchangeFunction;
import io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.RETRY_RESPONSE_CODE_REGEX;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Configuration holding an HttpClient and its configuration.
 * It is able to execute the HTTP call with retry if needed.
 * @param <C> type of the HttpClient
 * @param <NR> native HttpRequest
 * @param <NS> native HttpResponse
 */
@SuppressWarnings("java:S119")
//we don't want to use the generic of ConnectRecord, to handle both SinkRecord and SourceRecord
public class HttpConfiguration<C extends HttpClient<NR, NS>, NR, NS> implements Configuration<C,HttpRequest>,Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpConfiguration.class);
    public static final String RETRY_AFTER = "Retry-After";
    public static final String X_RETRY_AFTER = "X-Retry-After";
    public static final int INTERNAL_SERVER_ERROR = 503;
    public static final int TOO_MANY_REQUESTS = 429;
    public static final int MOVED_PERMANENTLY = 301;
    public static final String UTC = "UTC";

    private C client;
    private final ExecutorService executorService;
    private final RetryPolicy<HttpExchange> retryPolicy;
    @NotNull
    private final Map<String, String> settings;
    private final Pattern retryResponseCodeRegex;
    private final String id;
    private final Predicate<HttpRequest> predicate;
    private static final Pattern IS_INTEGER = Pattern.compile("\\d+");
    private static final DateTimeFormatter RFC_7231_FORMATTER = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss O");
    private static final DateTimeFormatter RFC_1123_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME;
    public HttpConfiguration(String id,
                             C client,
                             ExecutorService executorService,
                             RetryPolicy<HttpExchange> retryPolicy,
                             Map<String, String> settings) {
        this.id = id;
        this.client = client;
        this.executorService = executorService;
        this.retryPolicy = retryPolicy;
        this.settings = settings;
        //retry response code regex
        this.retryResponseCodeRegex = Pattern.compile(settings.getOrDefault(RETRY_RESPONSE_CODE_REGEX, DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX));

        this.predicate = HttpRequestPredicateBuilder.build().buildPredicate(settings);
    }

    public Pattern getRetryResponseCodeRegex() {
        return retryResponseCodeRegex;
    }

    private String predicateToString() {
        StringBuilder result = new StringBuilder("{");
        String urlRegex = settings.get(URL_REGEX);
        if(urlRegex!=null) {
            result.append("urlRegex:'").append(urlRegex).append("'");
        }
        String methodRegex = settings.get(METHOD_REGEX);
        if(methodRegex!=null) {
            result.append(",methodRegex:").append(methodRegex).append("'");
        }
        String bodyTypeRegex = settings.get(BODYTYPE_REGEX);
        if(bodyTypeRegex!=null) {
            result.append(",bodyTypeRegex:").append(bodyTypeRegex).append("'");
        }
        String headerKeyRegex = settings.get(HEADER_KEY_REGEX);
        if(headerKeyRegex!=null) {
            result.append(",headerKeyRegex:").append(headerKeyRegex).append("'");
        }
        result.append("}");
        return result.toString();
    }

    public RetryPolicy<HttpExchange> getRetryPolicy() {
        return retryPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        HttpConfiguration<?, ?, ?> that = (HttpConfiguration<?, ?, ?>) o;
        return Objects.equals(getClient(), that.getClient()) && Objects.equals(executorService, that.executorService) && Objects.equals(getRetryPolicy(), that.getRetryPolicy()) && Objects.equals(settings, that.settings) && Objects.equals(getRetryResponseCodeRegex().pattern(), that.getRetryResponseCodeRegex().pattern()) && Objects.equals(getId(), that.getId()) && Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClient(), executorService, getRetryPolicy(), settings, getRetryResponseCodeRegex(), getId(), predicate);
    }

    @Override
    public String toString() {
        return "HttpConfiguration{" +
                "httpClientConfiguration=" + client +
                ", executorService=" + executorService +
                ", settings=" + settings +
                ", retryResponseCodeRegex=" + retryResponseCodeRegex +
                ", id='" + id + '\'' +
                ", predicate=" + predicateToString() +
                '}';
    }

    /**
     *  - enrich request
     *  - execute the request
     * @param httpRequest HttpRequest to call
     * @param attempts current attempts before the call.
     * @return CompletableFuture of the HttpExchange (describing the request and response).
     */
    private CompletableFuture<HttpExchange> callAndEnrich(HttpRequest httpRequest,
                                                          AtomicInteger attempts) {
        attempts.addAndGet(HttpClient.ONE_HTTP_REQUEST);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("before enrichment:{}", httpRequest);
        }
        HttpRequest enrichedHttpRequest = enrich(httpRequest);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("after enrichment:{}", enrichedHttpRequest);
        }
        CompletableFuture<HttpExchange> completableFuture = this.client.call(enrichedHttpRequest, attempts);
        return completableFuture
                .thenApply(this::enrichHttpExchange);

    }
    /**
     * Call the web service with the given HttpRequest, and retry if needed.
     * @param httpRequest HttpRequest to call
     * @return CompletableFuture of the HttpExchange (describing the request and response).
     */
    public CompletableFuture<HttpExchange> call(@NotNull HttpRequest httpRequest) {
        Optional<RetryPolicy<HttpExchange>> retryPolicyForCall = Optional.ofNullable(getRetryPolicy());
        AtomicInteger attempts = new AtomicInteger();
        try {
            //a RetryPolicy is set
            if (retryPolicyForCall.isPresent()) {
                RetryPolicy<HttpExchange> myRetryPolicy = retryPolicyForCall.get();
                CircuitBreaker<HttpExchange> tooLongRetryDelayCircuitBreaker = CircuitBreaker.<HttpExchange>builder()
                        .handle(TooLongRetryDelayException.class)
                        //we break circuit when 1 TooLongRetryDelayException occurs
                        .withFailureThreshold(1)
                        //we reestablish the circuit after one successful call
                        .withSuccessThreshold(1)
                        .withDelayFnOn(context -> {
                            HttpExchange httpExchange = context.getLastResult();
                            HttpResponse response = httpExchange.getResponse();

                            Integer statusCode = response.getStatusCode();
                            LOGGER.debug("status code:{}",statusCode);

                            String retryAfterValue = getRetryAfterValue(response.getHeaders());
                            LOGGER.debug("Retry-After Value:{}",retryAfterValue);

                            long secondsToWait = getSecondsToWait(retryAfterValue);
                            LOGGER.debug("seconds to wait:{}",secondsToWait);

                            return Duration.of(secondsToWait, SECONDS);
                        },TooLongRetryDelayException.class)
                        .onOpen(context -> LOGGER.error("Circuit breaker for too long retry delay is now OPEN. Calls will not be retried anymore."))
                        .onHalfOpen(context -> LOGGER.info("Circuit breaker for too long retry delay is now HALF-OPEN. Next call will test the connection."))
                        .onClose(context -> LOGGER.warn("Circuit breaker for too long retry delay is now CLOSED. Calls can be retried again."))
                        .build();
                FailsafeExecutor<HttpExchange> failsafeExecutor = Failsafe.with(myRetryPolicy, tooLongRetryDelayCircuitBreaker);
                if (this.executorService != null) {
                    failsafeExecutor = failsafeExecutor.with(this.executorService);
                }
                return failsafeExecutor
                        .getStageAsync(ctx -> callAndEnrich(httpRequest, attempts)
                                .thenApply(this::handleRetry));
            } else {
                //no RetryPolicy is set
                return callAndEnrich(httpRequest, attempts);
            }
        } catch (Exception exception) {
            LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, exception,
                    exception.getMessage());
            HttpExchange httpExchange = getClient().buildExchange(
                    httpRequest,
                    new HttpResponse(HttpClient.SERVER_ERROR_STATUS_CODE, String.valueOf(exception.getMessage())),
                    Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    attempts,
                    HttpClient.FAILURE,
                    Maps.newHashMap(),
                    Maps.newHashMap());
            return CompletableFuture.supplyAsync(() -> httpExchange);
        }
    }
    /**
     * Handle the retry logic for the HttpExchange.
     * If the HttpExchange is successful, it returns the HttpExchange as is.
     * If the HttpExchange is not successful and the response code implies a retry, it throws an HttpException.
     * @param httpExchange HttpExchange to handle
     * @return HttpExchange if no retry is needed
     */
    private HttpExchange handleRetry(HttpExchange httpExchange) {
        //we don't retry successful HTTP Exchange
        boolean responseCodeImpliesRetry = retryNeeded(httpExchange.getResponse());
        LOGGER.debug("httpExchange success :'{}'", httpExchange.isSuccess());
        LOGGER.debug("response code('{}') implies retry:'{}'", httpExchange.getResponse().getStatusCode(), responseCodeImpliesRetry);
        if (!httpExchange.isSuccess() && responseCodeImpliesRetry) {
            throw new RetryException(httpExchange, "retry needed");
        }
        return httpExchange;
    }

    protected boolean retryNeeded(HttpResponse httpResponse) {
        Optional<Pattern> myRetryResponseCodeRegex = Optional.ofNullable(getRetryResponseCodeRegex());
        if (myRetryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = myRetryResponseCodeRegex.get();
            Integer statusCode = httpResponse.getStatusCode();
            Map<String, List<String>> httpResponseHeaders = httpResponse.getHeaders();
            if(httpResponseHeaders.containsKey(RETRY_AFTER)||httpResponseHeaders.containsKey(X_RETRY_AFTER)){
                return openCircuitWithRetryAfterHeader(httpResponseHeaders, statusCode);
            }
            Matcher matcher = retryPattern.matcher("" + statusCode);
            return matcher.matches();
        } else {
            return false;
        }
    }

    private boolean openCircuitWithRetryAfterHeader(Map<String, List<String>> httpResponseHeaders, Integer statusCode) {

        //log Response Status Code
        switch (statusCode){
            case INTERNAL_SERVER_ERROR: {
                //503 Internal server error : server's resources are exhausted
                LOGGER.debug("Internal server error : server's resources are exhausted");
                break;
            }
            case TOO_MANY_REQUESTS: {
                //429 too many requests
                LOGGER.debug("quota is exhausted : too many requests");
                break;
            }
            case MOVED_PERMANENTLY:{
                // 301 Moved Permanently
                LOGGER.debug(" 301 Moved Permanently");
                break;
            }
            default:
                //unknown code
        }
        String value = getRetryAfterValue(httpResponseHeaders);
        if(value==null){
            throw new IllegalStateException("there must be a 'Retry-After' or 'X-Retry-After' header" );
        }
        long secondsToWait = getSecondsToWait(value);
        LOGGER.debug("Retry-After or X-Retry-After header is present with value '{}', so delayed retry is needed",value);

        if (secondsToWait == 0L) return false;
        long retryDelayThreshold=60;
        if(secondsToWait>retryDelayThreshold){
            throw new TooLongRetryDelayException(secondsToWait,retryDelayThreshold);
        }else {
            try {
                //seconds to millis
                Thread.sleep(secondsToWait*1000);
                return true;
            } catch (InterruptedException e) {
                throw new HttpException(e);
            }
        }
    }

    private  long getSecondsToWait(String value) {
        //is it a date or an integer ?
        long secondsToWait;
        Instant until;

        if(IS_INTEGER.matcher(value).matches()){
            secondsToWait = Integer.parseInt(value);
        }else{
            try {
                until = LocalDateTime.parse(value, RFC_7231_FORMATTER).atZone(ZoneId.of(UTC)).toInstant();
            }catch (DateTimeParseException dtp){
                LOGGER.warn("Cannot parse Retry-After / X-Retry-After header value '{}' as a date with RFC7231 format, falling back to RFC1123 format", value);
                try {
                    until = ZonedDateTime.parse(value, RFC_1123_FORMATTER).toInstant();
                } catch (DateTimeParseException dtp2){
                    LOGGER.error("Cannot parse Retry-After / X-Retry-After header value '{}' as a date with RFC1123 format either, falling back retry", value);
                    return 0L;
                }
            }
            secondsToWait = Instant.now().until(until, SECONDS);

        }
        return secondsToWait;
    }

    private String getRetryAfterValue(Map<String, List<String>> httpResponseHeaders) {
        return httpResponseHeaders.get(RETRY_AFTER) != null ? httpResponseHeaders.get(RETRY_AFTER).get(0) : (httpResponseHeaders.get(X_RETRY_AFTER)!=null?httpResponseHeaders.get(X_RETRY_AFTER).get(0):null);
    }

    protected HttpRequest enrich(HttpRequest httpRequest) {
        return this.client.getEnrichRequestFunction().apply(httpRequest);
    }


    protected HttpExchange enrichHttpExchange(HttpExchange httpExchange) {
        AddSuccessStatusToHttpExchangeFunction addSuccessStatusToHttpExchangeFunction = client.getAddSuccessStatusToHttpExchangeFunction();
        return addSuccessStatusToHttpExchangeFunction!=null?addSuccessStatusToHttpExchangeFunction.apply(httpExchange):httpExchange;
    }
    @Override
    public boolean matches(HttpRequest httpRequest) {
        return this.predicate.test(httpRequest);
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public C getClient() {
        return this.client;
    }

    @Override
    public void setClient(C client) {
        if (this.client == null) {
            throw new IllegalStateException("client is null, cannot set it");
        }
        this.client = client;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new HttpConfiguration<>(this.id, this.client, this.executorService, this.retryPolicy, Maps.newHashMap(this.settings));
    }
}
