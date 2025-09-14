package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
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

import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.*;
import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.HEADER_KEY_REGEX;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.RETRY_RESPONSE_CODE_REGEX;

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

    private C client;
    private final ExecutorService executorService;
    private final RetryPolicy<HttpExchange> retryPolicy;
    @NotNull
    private final Map<String, String> settings;
    private final Pattern retryResponseCodeRegex;
    private final String id;
    private final Predicate<HttpRequest> predicate;

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
        if (settings.containsKey(RETRY_RESPONSE_CODE_REGEX)) {
            this.retryResponseCodeRegex = Pattern.compile(settings.get(RETRY_RESPONSE_CODE_REGEX));
        }else {
            this.retryResponseCodeRegex = Pattern.compile(DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX);
        }

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
        String bodytypeRegex = settings.get(BODYTYPE_REGEX);
        if(bodytypeRegex!=null) {
            result.append(",bodytypeRegex:").append(bodytypeRegex).append("'");
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

            if (retryPolicyForCall.isPresent()) {
                RetryPolicy<HttpExchange> myRetryPolicy = retryPolicyForCall.get();
                FailsafeExecutor<HttpExchange> failsafeExecutor = Failsafe.with(List.of(myRetryPolicy));
                if (this.executorService != null) {
                    failsafeExecutor = failsafeExecutor.with(this.executorService);
                }
                return failsafeExecutor
                        .getStageAsync(ctx -> callAndEnrich(httpRequest, attempts)
                                .thenApply(this::handleRetry));
            } else {
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
        //we don't retry success HTTP Exchange
        boolean responseCodeImpliesRetry = retryNeeded(httpExchange.getHttpResponse());
        LOGGER.debug("httpExchange success :'{}'", httpExchange.isSuccess());
        LOGGER.debug("response code('{}') implies retry:'{}'", httpExchange.getHttpResponse().getStatusCode(), responseCodeImpliesRetry);
        if (!httpExchange.isSuccess() && responseCodeImpliesRetry) {
            throw new HttpException(httpExchange, "retry needed");
        }
        return httpExchange;
    }

    protected boolean retryNeeded(HttpResponse httpResponse) {
        Optional<Pattern> myRetryResponseCodeRegex = Optional.ofNullable(getRetryResponseCodeRegex());
        if (myRetryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = myRetryResponseCodeRegex.get();
            Matcher matcher = retryPattern.matcher("" + httpResponse.getStatusCode());
            return matcher.matches();
        } else {
            return false;
        }
    }

    protected HttpRequest enrich(HttpRequest httpRequest) {
        return this.client.getEnrichRequestFunction().apply(httpRequest);
    }


    protected HttpExchange enrichHttpExchange(HttpExchange httpExchange) {
        C client = this.getClient();
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
        HttpConfiguration<C, NR, NS> httpConfiguration = new HttpConfiguration<>(this.id, this.client, this.executorService, this.retryPolicy, Maps.newHashMap(this.settings));
        return httpConfiguration;
    }
}
