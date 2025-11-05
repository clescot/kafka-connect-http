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

import static com.google.common.primitives.Longs.min;
import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
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
    private Pattern customStatusCodeForRetryAfterHeader;

    public static final String USUAL_RETRY_AFTER_STATUS_CODES = "503|429|301";
    //cf https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
    //regex to match 503 (Internal Server Error), 429(Too Many Requests), 301(Moved Permanently) HTTP response status code
    public static final String RFC_7231_PATTERN = "EEE, dd MMM yyyy HH:mm:ss O";
    private static final DateTimeFormatter RFC_7231_FORMATTER = DateTimeFormatter.ofPattern(RFC_7231_PATTERN);
    private static final DateTimeFormatter RFC_1123_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME;
    private final long maxSecondsToWait;
    private final long retryDelayThreshold;
    private boolean closed = true;
    private Instant nextRetryInstant;
    private FailsafeExecutor<HttpExchange> failsafeExecutor;

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
        maxSecondsToWait = Long.parseLong(settings.getOrDefault(RETRY_AFTER_MAX_DURATION_IN_SEC, DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC));
        retryDelayThreshold = Long.parseLong(settings.getOrDefault(RETRY_DELAY_THRESHOLD_IN_SEC, DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC));
        customStatusCodeForRetryAfterHeader = Pattern.compile(settings.getOrDefault(CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER, DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER));
        failsafeExecutor = buildFailsafeExecutor();
    }

    private FailsafeExecutor<HttpExchange> buildFailsafeExecutor() {
        Optional<RetryPolicy<HttpExchange>> retryPolicyForCall = Optional.ofNullable(getRetryPolicy());
        //a RetryPolicy is set
        if (retryPolicyForCall.isPresent()) {
            RetryPolicy<HttpExchange> myRetryPolicy = retryPolicyForCall.get();
            CircuitBreaker<HttpExchange> circuitBreaker = buildCircuitBreaker();
            //compose policies
            FailsafeExecutor<HttpExchange> failsafeExecutor = Failsafe.with(myRetryPolicy, circuitBreaker);
            if (this.executorService != null) {
                failsafeExecutor = failsafeExecutor.with(this.executorService);
            }
            return failsafeExecutor;
        } else {
            //no RetryPolicy is set
            return null;
        }
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
     * indicates if the client is closed (i.e enabled) due to circuit breaker opening.
     * @return true if the client is closed.
     */
    public boolean isClosed() {
        return closed;
    }

    public long getMaxSecondsToWait() {
        return maxSecondsToWait;
    }

    public long getRetryDelayThreshold() {
        return retryDelayThreshold;
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
        attempts.addAndGet(HttpClient.ONE_REQUEST);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("before enrichment:{}", httpRequest);
        }
        HttpRequest enrichedHttpRequest = enrichRequest(httpRequest);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("after enrichment:{}", enrichedHttpRequest);
        }
        CompletableFuture<HttpExchange> completableFuture = this.client.call(enrichedHttpRequest, attempts);
        return completableFuture
                .thenApply(this::enrichExchange);

    }
    /**
     * Call the web service with the given HttpRequest, and retry if needed.
     * @param httpRequest HttpRequest to call
     * @return CompletableFuture of the HttpExchange (describing the request and response).
     */
    public CompletableFuture<HttpExchange> call(@NotNull HttpRequest httpRequest) {
        AtomicInteger attempts = new AtomicInteger();
        try {
            //a RetryPolicy is set
            if (failsafeExecutor!=null) {
                return failsafeExecutor
                        .getStageAsync(ctx -> callAndEnrich(httpRequest, attempts)
                                .thenApply(this::handleRetry));
            } else {
                //no RetryPolicy is set
                return callAndEnrich(httpRequest, attempts);
            }
        } catch (TooLongRetryDelayException tooLongRetryDelayException) {
            //Retry-After delay is too long
            LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, tooLongRetryDelayException,
                    tooLongRetryDelayException.getMessage());
            HttpExchange httpExchange = getClient().buildExchange(
                    httpRequest,
                    tooLongRetryDelayException.getHttpExchange().getResponse(),
                    Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    attempts,
                    Maps.newHashMap(),
                    Maps.newHashMap());
            return CompletableFuture.supplyAsync(() -> httpExchange);
        } catch (Exception exception) {
            LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, exception,
                    exception.getMessage());
            HttpExchange httpExchange = getClient().buildExchange(
                    httpRequest,
                    new HttpResponse(HttpClient.SERVER_ERROR_STATUS_CODE, String.valueOf(exception.getMessage())),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    attempts,
                    Maps.newHashMap(),
                    Maps.newHashMap());
            return CompletableFuture.supplyAsync(() -> httpExchange);
        }
    }

    private CircuitBreaker<HttpExchange> buildCircuitBreaker() {
        return CircuitBreaker.<HttpExchange>builder()
                .handleResultIf(httpExchange -> getRetryAfterValue(httpExchange.getResponse().getHeaders())!=null)
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

                    return Duration.of( min(secondsToWait,maxSecondsToWait), SECONDS);
                },TooLongRetryDelayException.class)
                .handleIf(failure -> {
                    if (failure instanceof TooLongRetryDelayException tooLongRetryDelayException) {
                        LOGGER.error("Circuit breaker detected a too long retry delay of {} seconds exceeding the threshold of {} seconds. Opening the circuit.",
                                tooLongRetryDelayException.getSecondsToWait(), tooLongRetryDelayException.getRetryDelayThreshold());
                        this.nextRetryInstant = tooLongRetryDelayException.getNextRetryInstant();
                        return true;
                    }
                    return false;
                })
                .onOpen(context ->{
                    LOGGER.error("Circuit breaker for too long retry delay is now OPEN. Calls will not be retried anymore.");
                    closed = false;
                })
                .onHalfOpen(context -> {
                    LOGGER.info("Circuit breaker for too long retry delay is now HALF-OPEN. Next call will test the connection.");
                    closed = true;
                    this.nextRetryInstant = null;
                })
                .onClose(context -> LOGGER.warn("Circuit breaker for too long retry delay is now CLOSED. Calls can be retried again."))
                .build();
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
        boolean responseCodeImpliesRetry = retryNeeded(httpExchange);
        LOGGER.debug("httpExchange success :'{}'", httpExchange.isSuccess());
        LOGGER.debug("response code('{}') implies retry:'{}'", httpExchange.getResponse().getStatusCode(), responseCodeImpliesRetry);
        if (!httpExchange.isSuccess() && responseCodeImpliesRetry) {
            throw new RetryException(httpExchange, "retry needed");
        }
        return httpExchange;
    }

    protected boolean retryNeeded(HttpExchange httpExchange) {
        Optional<Pattern> myRetryResponseCodeRegex = Optional.ofNullable(getRetryResponseCodeRegex());
        if (myRetryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = myRetryResponseCodeRegex.get();
            HttpResponse response = httpExchange.getResponse();
            int statusCode = response.getStatusCode();
            Map<String, List<String>> httpResponseHeaders = response.getHeaders();
            if(httpResponseHeaders.containsKey(RETRY_AFTER)||httpResponseHeaders.containsKey(X_RETRY_AFTER)){
                return openCircuitWithRetryAfterHeader(httpExchange);
            }
            Matcher matcher = retryPattern.matcher("" + statusCode);
            return matcher.matches();
        } else {
            return false;
        }
    }

    /**
     * when the response code is compatible with a retry after header, we can open the circuit (i.e disable the client),
     * and wait for the retry after header Duration to re-enable the client.
     *
     * @param httpExchange the exchange
     * @return true if the circuit must be opened (i.e the client must be disabled), false otherwise
     */
    private boolean openCircuitWithRetryAfterHeader(HttpExchange httpExchange) {

        HttpResponse response = httpExchange.getResponse();
        Integer statusCode = response.getStatusCode();
        boolean statusCodeIsCompatibleWithRetryAfter = statusCodeIsCompatibleWithRetryAfter(statusCode);
        if(!statusCodeIsCompatibleWithRetryAfter){
            return false;
        }
        String value = getRetryAfterValue(httpExchange.getResponse().getHeaders());
        if(value==null){
            throw new IllegalStateException("there must be a 'Retry-After' or 'X-Retry-After' header" );
        }
        long secondsToWait = getSecondsToWait(value);
        LOGGER.debug("Retry-After or X-Retry-After header is present with value '{}', so delayed retry is needed",value);

        if (secondsToWait == 0L) return false;

        if(secondsToWait>retryDelayThreshold){
            throw new TooLongRetryDelayException(httpExchange,secondsToWait,retryDelayThreshold);
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

    private boolean statusCodeIsCompatibleWithRetryAfter(Integer statusCode) {
        return customStatusCodeForRetryAfterHeader.matcher(""+statusCode).matches();
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

    protected HttpRequest enrichRequest(HttpRequest httpRequest) {
        return this.client.getEnrichRequestFunction().apply(httpRequest);
    }


    protected HttpExchange enrichExchange(HttpExchange httpExchange) {
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

    public Instant getNextRetryInstant() {
        return nextRetryInstant;
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
