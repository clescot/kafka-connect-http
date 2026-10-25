package io.github.clescot.client.http;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import dev.failsafe.CircuitBreaker;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.github.clescot.client.Client;
import io.github.clescot.client.Configuration;
import io.github.clescot.client.RequestClient;
import io.github.clescot.client.RequestResponseClient;
import io.github.clescot.client.http.config.AddSuccessStatusToHttpExchangeFunction;
import io.github.clescot.client.http.config.HttpRequestPredicateBuilder;
import io.github.clescot.client.http.ratelimit.RateLimitGate;
import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
import io.github.clescot.core.http.template.ExchangeTemplateManager;
import io.github.clescot.core.http.template.ExchangeTemplateProcessorFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.primitives.Longs.min;
import static io.github.clescot.client.Constants.*;
import static io.github.clescot.client.http.config.HttpRequestPredicateBuilder.*;
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
    public static final int TOO_MANY_REQUESTS_STATUS_CODE = 429;

    private C client;
    private final ExecutorService executorService;
    private final RetryPolicy<HttpExchange> retryPolicy;
    @NotNull
    private final Map<String, String> settings;
    private final Pattern retryResponseCodeRegex;
    private final String id;
    private final Predicate<HttpRequest> predicate;
    private final RateLimitGate rateLimitGate;

    private final Pattern customStatusCodeForRetryAfterHeader;
    private final long maxSecondsToWait;
    private final long retryDelayThreshold;
    private Instant nextRetryInstant;
    private final FailsafeExecutor<HttpExchange> failsafeExecutor;
    private final long defaultRetryAfterDelayInSeconds;
    private ExchangeTemplateManager templateManager;
    private String exchangeTemplate;
    private ExchangeTemplateProcessorFactory factory = new ExchangeTemplateProcessorFactory();

    public HttpConfiguration(String id,
                             C client,
                             ExecutorService executorService,
                             RetryPolicy<HttpExchange> retryPolicy,
                             Map<String, String> settings) {
        this(id, client, executorService, retryPolicy, settings, RateLimitGate.disabled(id));
    }

    public HttpConfiguration(String id,
                             C client,
                             ExecutorService executorService,
                             RetryPolicy<HttpExchange> retryPolicy,
                             Map<String, String> settings,
                             RateLimitGate rateLimitGate) {
        this.id = id;
        this.client = client;
        this.executorService = executorService;
        this.retryPolicy = retryPolicy;
        this.settings = settings;
        this.rateLimitGate = rateLimitGate == null ? RateLimitGate.disabled(id) : rateLimitGate;
        //retry response code regex
        this.retryResponseCodeRegex = Pattern.compile(settings.getOrDefault(RETRY_RESPONSE_CODE_REGEX, DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX));

        this.predicate = HttpRequestPredicateBuilder.build().buildPredicate(settings);
        maxSecondsToWait = Long.parseLong(settings.getOrDefault(RETRY_AFTER_MAX_DURATION_IN_SEC, DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC));
        retryDelayThreshold = Long.parseLong(settings.getOrDefault(RETRY_DELAY_THRESHOLD_IN_SEC, DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC));
        customStatusCodeForRetryAfterHeader = Pattern.compile(settings.getOrDefault(CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER, DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER));
        defaultRetryAfterDelayInSeconds = Long.parseLong(settings.getOrDefault(DEFAULT_RETRY_DELAY_IN_SEC, DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC));
        failsafeExecutor = buildFailsafeExecutor();

        // Initialize template processing using shared utility
        this.exchangeTemplate = settings.getOrDefault(EXCHANGE_TEMPLATE, "");
        this.templateManager = factory.createTemplateManager(settings);
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
        long rateLimitWaitingTime = rateLimitGate.awaitIfExhausted(httpRequest.getUrl());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("before enrichment:{}", httpRequest);
        }
        HttpRequest enrichedHttpRequest = enrichRequest(httpRequest);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("after enrichment:{}", enrichedHttpRequest);
        }
        CompletableFuture<HttpExchange> completableFuture = this.client.call(enrichedHttpRequest, attempts);
        return completableFuture
                .thenApply(this::enrichExchange)
                .thenApply(httpExchange -> addRateLimitWaitingTime(httpExchange, rateLimitWaitingTime));

    }

    private HttpExchange addRateLimitWaitingTime(HttpExchange httpExchange, long rateLimitWaitingTime) {
        if (rateLimitWaitingTime > 0 && httpExchange.getTimings() != null) {
            httpExchange.getTimings().put(RateLimitGate.RATE_LIMIT_WAITING_TIME_TIMING_KEY, rateLimitWaitingTime);
        }
        return httpExchange;
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
                //we break the circuit when the advertised delay or exhausted window exceeds the
                //retry delay threshold (see circuitMustBeOpened)
                .withFailureThreshold(1)
                //we reestablish the circuit after one successful call
                .withSuccessThreshold(1)
                //only results open the circuit breaker (through circuitMustBeOpened): a failure
                //condition must be configured, otherwise every exception would count as a failure
                .handleIf((Throwable throwable) -> false)
                .withDelayFn(context -> {
                    HttpExchange httpExchange = context.getLastResult();
                    String retryAfterValue = null;
                    if (httpExchange != null && httpExchange.getResponse() != null) {
                        retryAfterValue = httpExchange.getResponse().getRetryAfterValue();
                    }
                    LOGGER.debug("Retry-After Value:{}", retryAfterValue);

                    long secondsToWait = delayBeforeClosingTheCircuit(httpExchange);
                    LOGGER.debug("seconds to wait:{}", secondsToWait);
                    this.nextRetryInstant = Instant.now().plusSeconds(secondsToWait);
                    if (httpExchange != null && httpExchange.getRequest() != null) {
                        httpExchange.getRequest().setRetryAfterInstant(this.nextRetryInstant);
                    }
                    LOGGER.info("Circuit breaker opened for '{}' seconds, until '{}'", secondsToWait, nextRetryInstant);
                    return Duration.of(min(secondsToWait, maxSecondsToWait), SECONDS);
                })
                .handleResultIf(result -> circuitMustBeOpened(result) > 0L)
                .onOpen(context -> LOGGER.error("Circuit breaker for too long retry delay is now OPEN. Calls will not be retried anymore."))
                .onHalfOpen(context -> {
                    LOGGER.info("Circuit breaker for too long retry delay is now HALF-OPEN. Next call will test the connection.");
                    this.nextRetryInstant = null;
                })
                .onClose(context -> LOGGER.warn("Circuit breaker for too long retry delay is now CLOSED. Calls can be retried again."))
                .build();
    }

    private long delayBeforeClosingTheCircuit(HttpExchange httpExchange) {
        if (httpExchange == null || httpExchange.getResponse() == null) {
            return defaultRetryAfterDelayInSeconds;
        }
        HttpResponse response = httpExchange.getResponse();
        String retryAfterValue = response.getRetryAfterValue();
        if (retryAfterValue != null) {
            return response.getRetryAfterSecondsToWait(retryAfterValue);
        }
        long rateLimitWait = rateLimitGate.exhaustedWaitSeconds(httpExchange);
        if (rateLimitWait > 0) {
            return rateLimitWait;
        }
        return defaultRetryAfterDelayInSeconds;
    }

    /**
     * Handle the retry logic for the HttpExchange.
     * If the HttpExchange is successful, it returns the HttpExchange as is.
     * If the HttpExchange is not successful and the response code implies a retry, it throws an HttpException.
     *
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

    /**
     * Check if the response code implies a retry.
     *
     * @param httpExchange HttpExchange to check
     * @return true if the response code implies a retry
     */
    protected boolean retryNeeded(HttpExchange httpExchange) {
        Optional<Pattern> myRetryResponseCodeRegex = Optional.ofNullable(getRetryResponseCodeRegex());
        if (myRetryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = myRetryResponseCodeRegex.get();
            HttpResponse response = httpExchange.getResponse();
            int statusCode = response.getStatusCode();
            Map<String, List<String>> httpResponseHeaders = response.getHeaders();
            boolean hasRetryAfter = httpResponseHeaders.containsKey(RETRY_AFTER) || httpResponseHeaders.containsKey(X_RETRY_AFTER);
            //an exhausted server-advertised quota is a delay instruction only on the status codes
            //that already treat a server delay as a retry instruction
            long rateLimitWait = rateLimitGate.exhaustedWaitSeconds(httpExchange);
            boolean rateLimitAdvisesDelayRetry = rateLimitWait > 0 && statusCodeIsCompatibleWithRetryAfter(statusCode);
            if (hasRetryAfter || rateLimitAdvisesDelayRetry) {
                long secondToWait = hasRetryAfter ? circuitMustBeOpened(httpExchange) : rateLimitWait;
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
     * when the response advertises a delay, we can open the circuit (i.e disable the client),
     * and wait for that delay to re-enable the client. The delay is either a Retry-After /
     * X-Retry-After header value, or the window of an exhausted server-advertised RateLimit
     * quota.
     *
     * @param httpExchange the exchange with the response carrying the delay information
     * @return the number of seconds to wait before re-enabling the client, 0 if no need to open the circuit.
     */
    private long circuitMustBeOpened(HttpExchange httpExchange) {

        HttpResponse response = httpExchange.getResponse();
        Integer statusCode = response.getStatusCode();
        String value = response.getRetryAfterValue();
        boolean statusCodeIsCompatibleWithRetryAfter = statusCodeIsCompatibleWithRetryAfter(statusCode);
        if (value != null) {
            if (!statusCodeIsCompatibleWithRetryAfter) {
                //Retry-After on a status code the connector does not treat as a retry instruction
                return 0L;
            }
            long secondsToWait = response.getRetryAfterSecondsToWait(value);
            LOGGER.debug("Retry-After or X-Retry-After header is present with value '{}', so delayed retry is needed", value);
            if (secondsToWait == 0L) {
                return 0L;
            }
            if (secondsToWait > retryDelayThreshold) {
                return secondsToWait;
            } else {
                //delay is not too long to wait
                LOGGER.info("Waiting '{}' seconds (below the retryDelayThreshold:'{}' seconds) before retrying the call", secondsToWait, retryDelayThreshold);
                return secondsToWait;
            }
        }
        //no Retry-After: an exhausted server-advertised quota can open the breaker, whatever the
        //status code the fields came with (the IETF draft allows them on any response)
        long rateLimitWait = rateLimitGate.exhaustedWaitSeconds(httpExchange);
        if (rateLimitWait > retryDelayThreshold) {
            LOGGER.info("Advertised rate limit exhausted for '{}' seconds (over the retryDelayThreshold:'{}' seconds), opening the circuit breaker", rateLimitWait, retryDelayThreshold);
            return rateLimitWait;
        }
        if (rateLimitWait > 0) {
            //below the threshold the gate waits before the next call; the circuit breaker stays closed
            return 0L;
        }
        //status code 429 is clear : we need to retry after a delay, although if no delay is present in headers
        if (statusCode != null && statusCode == TOO_MANY_REQUESTS_STATUS_CODE && statusCodeIsCompatibleWithRetryAfter) {
            LOGGER.debug("429 status code detected without Retry-After, X-Retry-After or exhausted RateLimit, falling back to default retry value '{}' seconds", defaultRetryAfterDelayInSeconds);
            return defaultRetryAfterDelayInSeconds;
        }
        return 0L;
    }

    private boolean statusCodeIsCompatibleWithRetryAfter(Integer statusCode) {
        return customStatusCodeForRetryAfterHeader.matcher("" + statusCode).matches();
    }



    protected HttpRequest enrichRequest(HttpRequest httpRequest) {
        return this.client.getEnrichRequestFunction().apply(httpRequest);
    }


    protected HttpExchange enrichExchange(HttpExchange httpExchange) {
        HttpExchange enrichedExchange = httpExchange;

        // Apply success status function if available
        AddSuccessStatusToHttpExchangeFunction addSuccessStatusToHttpExchangeFunction = client.getAddSuccessStatusToHttpExchangeFunction();
        if (addSuccessStatusToHttpExchangeFunction != null) {
            enrichedExchange = addSuccessStatusToHttpExchangeFunction.apply(enrichedExchange);
        }

        //remember an exhausted server-advertised quota for the next calls to the same authority
        rateLimitGate.observe(enrichedExchange);

        // Apply template processing if template is configured
        if (exchangeTemplate != null && !exchangeTemplate.trim().isEmpty() && templateManager != null) {
            try {
                String resolvedTemplate = templateManager.resolveTemplate(enrichedExchange, exchangeTemplate, Collections.emptyMap());
                if (resolvedTemplate != null && !resolvedTemplate.equals(exchangeTemplate)) {
                    // Set the resolved content on the exchange
                    HttpResponse response = enrichedExchange.getResponse();
                    if (response != null) {
                        HttpResponse newResponse = (HttpResponse) response.clone();
                        newResponse.setBodyAsString(resolvedTemplate);
                        enrichedExchange = HttpExchange.Builder.anHttpExchange()
                                .withHttpRequest(enrichedExchange.getRequest())
                                .withHttpResponse(newResponse)
                                .withDuration(enrichedExchange.getDurationInMillis())
                                .at(enrichedExchange.getMoment())
                                .withAttempts(enrichedExchange.getAttempts())
                                .withAttributes(new HashMap<>(enrichedExchange.getAttributes()))
                                .withTimings(new HashMap<>(enrichedExchange.getTimings()))
                                .build();
                        LOGGER.debug("Applied template processing to HttpExchange, resolved to: {}", resolvedTemplate);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to apply template processing: {}", e.getMessage());
            }
        }

        return enrichedExchange;
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

    public String getExchangeTemplate() {
        return exchangeTemplate;
    }

    public void setExchangeTemplate(String exchangeTemplate) {
        this.exchangeTemplate = exchangeTemplate;
    }

    public ExchangeTemplateManager getTemplateManager() {
        return templateManager;
    }

    public void setTemplateManager(ExchangeTemplateManager templateManager) {
        this.templateManager = templateManager;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new HttpConfiguration<>(this.id, this.client, this.executorService, this.retryPolicy, Maps.newHashMap(this.settings), this.rateLimitGate);
    }
}
