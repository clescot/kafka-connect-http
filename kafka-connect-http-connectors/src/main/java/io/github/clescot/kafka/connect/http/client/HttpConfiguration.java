package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import dev.failsafe.CircuitBreaker;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.RequestClient;
import io.github.clescot.kafka.connect.RequestResponseClient;
import io.github.clescot.kafka.connect.http.client.config.AddSuccessStatusToHttpExchangeFunction;
import io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
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
 *
 * @param <C>  type of the HttpClient
 * @param <NR> native HttpRequest
 * @param <NS> native HttpResponse
 */
@SuppressWarnings("java:S119")
//we don't want to use the generic of ConnectRecord, to handle both SinkRecord and SourceRecord
public class HttpConfiguration<C extends HttpClient<NR, NS>, NR, NS> implements Configuration<C, HttpRequest>, Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpConfiguration.class);
    public static final String RETRY_AFTER = "Retry-After";
    public static final String X_RETRY_AFTER = "X-Retry-After";

    private C client;
    private final ExecutorService executorService;
    private final RetryPolicy<HttpExchange> retryPolicy;
    @NotNull
    private final Map<String, String> settings;
    private final Pattern retryResponseCodeRegex;
    private final String id;
    private final Predicate<HttpRequest> predicate;

    private final Pattern customStatusCodeForRetryAfterHeader;
    private final long maxSecondsToWait;
    private final long retryDelayThreshold;
    private Instant nextRetryInstant;
    private final FailsafeExecutor<HttpExchange> failsafeExecutor;
    private final long defaultRetryAfterDelayInSeconds;

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
        defaultRetryAfterDelayInSeconds = Long.parseLong(settings.getOrDefault(DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC, DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC));
        failsafeExecutor = buildFailsafeExecutor();
    }

    private FailsafeExecutor<HttpExchange> buildFailsafeExecutor() {
        Optional<RetryPolicy<HttpExchange>> retryPolicyForCall = Optional.ofNullable(getRetryPolicy());
        //a RetryPolicy is set
        if (retryPolicyForCall.isPresent()) {
            RetryPolicy<HttpExchange> myRetryPolicy = retryPolicyForCall.get();
            CircuitBreaker<HttpExchange> circuitBreaker = buildCircuitBreaker();
            //compose policies
            FailsafeExecutor<HttpExchange> fsExecutor = Failsafe.with(myRetryPolicy, circuitBreaker);
            if (this.executorService != null) {
                fsExecutor = fsExecutor.with(this.executorService);
            }
            return fsExecutor;
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
        if (urlRegex != null) {
            result.append("urlRegex:'").append(urlRegex).append("'");
        }
        String methodRegex = settings.get(METHOD_REGEX);
        if (methodRegex != null) {
            result.append(",methodRegex:").append(methodRegex).append("'");
        }
        String bodyTypeRegex = settings.get(BODYTYPE_REGEX);
        if (bodyTypeRegex != null) {
            result.append(",bodyTypeRegex:").append(bodyTypeRegex).append("'");
        }
        String headerKeyRegex = settings.get(HEADER_KEY_REGEX);
        if (headerKeyRegex != null) {
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
     * indicates if the client is closed (i.e enabled) due to circuit breaker.
     *
     * @return true if the client is closed.
     */
    public boolean isClosed() {
        return nextRetryInstant == null || nextRetryInstant.isBefore(Instant.now());
    }

    /**
     * indicates if the client is open (i.e disabled) due to circuit breaker opening.
     *
     * @return true if the client is open.
     */
    public boolean isOpen() {
        return !isClosed();
    }


    public long getMaxSecondsToWait() {
        return maxSecondsToWait;
    }

    public long getRetryDelayThreshold() {
        return retryDelayThreshold;
    }

    /**
     * - enrich request
     * - execute the request
     *
     * @param httpRequest HttpRequest to call
     * @param attempts    current attempts before the call.
     * @return CompletableFuture of the HttpExchange (describing the request and response).
     */
    private CompletableFuture<HttpExchange> callAndEnrich(HttpRequest httpRequest,
                                                          AtomicInteger attempts) {
        attempts.addAndGet(RequestClient.ONE_REQUEST);
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
     *
     * @param httpRequest HttpRequest to call
     * @return CompletableFuture of the HttpExchange (describing the request and response).
     */
    public CompletableFuture<HttpExchange> call(@NotNull HttpRequest httpRequest) {
        AtomicInteger attempts = new AtomicInteger();
        try {
            //a RetryPolicy is set
            if (failsafeExecutor != null) {
                return failsafeExecutor
                        .getStageAsync(
                                ctx -> callAndEnrich(httpRequest, attempts)
                                        .thenApply(this::handleRetry)
                        );
            } else {
                //no RetryPolicy is set
                return callAndEnrich(httpRequest, attempts);
            }
        } catch (TooLongRetryDelayException tooLongRetryDelayException) {
            //Retry-After delay is too long
            LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ",
                    attempts,
                    tooLongRetryDelayException,
                    tooLongRetryDelayException.getMessage()
            );
            HttpExchange httpExchange = getClient().buildExchange(
                    httpRequest,
                    tooLongRetryDelayException.getHttpExchange().getResponse(),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(RequestResponseClient.UTC_ZONE_ID)),
                    attempts,
                    Maps.newHashMap(),
                    Maps.newHashMap());
            return CompletableFuture.supplyAsync(() -> httpExchange);
        } catch (Exception exception) {
            LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ",
                    attempts,
                    exception,
                    exception.getMessage());
            HttpExchange httpExchange = getClient().buildExchange(
                    httpRequest,
                    new HttpResponse(HttpClient.SERVER_ERROR_STATUS_CODE, String.valueOf(exception.getMessage())),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(RequestResponseClient.UTC_ZONE_ID)),
                    attempts,
                    Maps.newHashMap(),
                    Maps.newHashMap());
            return CompletableFuture.supplyAsync(() -> httpExchange);
        }
    }

    private CircuitBreaker<HttpExchange> buildCircuitBreaker() {
        return CircuitBreaker.<HttpExchange>builder()
                //we break circuit when 1 TooLongRetryDelayException occurs
                .withFailureThreshold(1)
                //we reestablish the circuit after one successful call
                .withSuccessThreshold(1)
                .withDelayFn(context -> {
                    HttpExchange httpExchange = context.getLastResult();
                    HttpResponse response = httpExchange.getResponse();

                    Integer statusCode = response.getStatusCode();
                    LOGGER.debug("status code:{}", statusCode);

                    String retryAfterValue = response.getRetryAfterValue();
                    LOGGER.debug("Retry-After Value:{}", retryAfterValue);

                    long secondsToWait = response.getRetryAfterSecondsToWait(MoreObjects.firstNonNull(retryAfterValue, DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC));
                    LOGGER.debug("seconds to wait:{}", secondsToWait);
                    this.nextRetryInstant = Instant.now().plusSeconds(secondsToWait);
                    httpExchange.getRequest().setRetryAfterInstant(this.nextRetryInstant);
                    LOGGER.info("Circuit breaker opened for '{}' seconds, until '{}'", secondsToWait, nextRetryInstant);
                    return Duration.of(min(secondsToWait, maxSecondsToWait), SECONDS);
                })
                .handle(TooLongRetryDelayException.class)
                .handleResultIf(result -> circuitMustBeOpened(result) > 0L)
                .onOpen(context -> LOGGER.error("Circuit breaker for too long retry delay is now OPEN. Calls will not be retried anymore."))
                .onHalfOpen(context -> {
                    LOGGER.info("Circuit breaker for too long retry delay is now HALF-OPEN. Next call will test the connection.");
                    this.nextRetryInstant = null;
                })
                .onClose(context -> LOGGER.warn("Circuit breaker for too long retry delay is now CLOSED. Calls can be retried again."))
                .build();
    }

    /**
     * Handle the retry logic for the HttpExchange.
     * If the HttpExchange is successful, it returns the HttpExchange as is.
     * If the HttpExchange is not successful and the response code implies a retry, it throws an HttpException.
     *
     * @param httpExchange HttpExchange to handle
     * @return HttpExchange if no retry is needed
     */
    private HttpExchange handleRetry(HttpExchange httpExchange) throws TooLongRetryDelayException {
        //we don't retry successful HTTP Exchange
        boolean responseCodeImpliesRetry = retryNeeded(httpExchange);
        LOGGER.debug("httpExchange success :'{}'", httpExchange.isSuccess());
        LOGGER.debug("response code('{}') implies retry:'{}'", httpExchange.getResponse().getStatusCode(), responseCodeImpliesRetry);
        if (!httpExchange.isSuccess() && responseCodeImpliesRetry) {
            throw new RetryException(httpExchange, "retry needed");
        }
        return httpExchange;
    }

    /**
     * Check if the response code implies a retry.
     *
     * @param httpExchange HttpExchange to check
     * @return true if the response code implies a retry
     */
    protected boolean retryNeeded(HttpExchange httpExchange) throws TooLongRetryDelayException {
        Optional<Pattern> myRetryResponseCodeRegex = Optional.ofNullable(getRetryResponseCodeRegex());
        if (myRetryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = myRetryResponseCodeRegex.get();
            HttpResponse response = httpExchange.getResponse();
            int statusCode = response.getStatusCode();
            Map<String, List<String>> httpResponseHeaders = response.getHeaders();
            if (httpResponseHeaders.containsKey(RETRY_AFTER) || httpResponseHeaders.containsKey(X_RETRY_AFTER)) {
                long secondToWait = circuitMustBeOpened(httpExchange);
                if (secondToWait < retryDelayThreshold) {
                    try {
                        LOGGER.info("Waiting '{}' seconds (below the retryDelayThreshold:'{}' seconds) before retrying the call", secondToWait, retryDelayThreshold);
                        Thread.sleep(secondToWait * 1000);
                        return true;
                    } catch (InterruptedException e) {
                        throw new HttpException(e);
                    }
                } else {
                    return false;
                }
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
     * @param httpExchange the exchange with the response containing the retry after header
     * @return the number of seconds to wait before re-enabling the client, 0 if no need to open the circuit.
     */
    private long circuitMustBeOpened(HttpExchange httpExchange) {

        HttpResponse response = httpExchange.getResponse();
        Integer statusCode = response.getStatusCode();
        //503,429,301 ?
        boolean statusCodeIsCompatibleWithRetryAfter = statusCodeIsCompatibleWithRetryAfter(statusCode);
        if (!statusCodeIsCompatibleWithRetryAfter) {
            return 0L;
        }
        String value = httpExchange.getResponse().getRetryAfterValue();
        if (value == null && statusCode != 429) {
            return 0L;
        }
        //status code 429 is clear : we need to retry after a delay, although if no delay is present in headers
        if (value == null) {
            LOGGER.debug("429 status code detected without Retry-After or X-Retry-After header, falling back to default retry value '{}' seconds", defaultRetryAfterDelayInSeconds);
            return defaultRetryAfterDelayInSeconds;
        }
        long secondsToWait = response.getRetryAfterSecondsToWait(value);
        LOGGER.debug("Retry-After or X-Retry-After header is present with value '{}', so delayed retry is needed", value);

        if (secondsToWait == 0L) return 0L;

        if (secondsToWait > retryDelayThreshold) {
            return secondsToWait;
        } else {
            //delay is not too long to wait
            //seconds to millis
            LOGGER.info("Waiting '{}' seconds (below the retryDelayThreshold:'{}' seconds) before retrying the call", secondsToWait, retryDelayThreshold);
            return secondsToWait;
        }
    }

    private boolean statusCodeIsCompatibleWithRetryAfter(Integer statusCode) {
        return customStatusCodeForRetryAfterHeader.matcher("" + statusCode).matches();
    }



    protected HttpRequest enrichRequest(HttpRequest httpRequest) {
        return this.client.getEnrichRequestFunction().apply(httpRequest);
    }


    protected HttpExchange enrichExchange(HttpExchange httpExchange) {
        AddSuccessStatusToHttpExchangeFunction addSuccessStatusToHttpExchangeFunction = client.getAddSuccessStatusToHttpExchangeFunction();
        return addSuccessStatusToHttpExchangeFunction != null ? addSuccessStatusToHttpExchangeFunction.apply(httpExchange) : httpExchange;
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
