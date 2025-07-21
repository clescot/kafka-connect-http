package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Stopwatch;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpConfiguration<C extends HttpClient<R, S>, R, S> implements Configuration<C,HttpRequest>{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpConfiguration.class);

    private final HttpClientConfiguration<C, R, S> httpClientConfiguration;

    public HttpConfiguration(HttpClientConfiguration<C, R, S> httpClientConfiguration) {
        this.httpClientConfiguration = httpClientConfiguration;
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
        CompletableFuture<HttpExchange> completableFuture = this.httpClientConfiguration.getClient().call(enrichedHttpRequest, attempts);
        return completableFuture
                .thenApply(this::enrichHttpExchange);

    }
    /**
     * Call the web service with the given HttpRequest, and retry if needed.
     * @param httpRequest HttpRequest to call
     * @return CompletableFuture of the HttpExchange (describing the request and response).
     */
    public CompletableFuture<HttpExchange> call(@NotNull HttpRequest httpRequest) {
        Optional<RetryPolicy<HttpExchange>> retryPolicyForCall = this.httpClientConfiguration.getRetryPolicy();
        AtomicInteger attempts = new AtomicInteger();
        try {

            if (retryPolicyForCall.isPresent()) {
                RetryPolicy<HttpExchange> myRetryPolicy = retryPolicyForCall.get();
                FailsafeExecutor<HttpExchange> failsafeExecutor = Failsafe.with(List.of(myRetryPolicy));
                if (this.httpClientConfiguration.getExecutorService() != null) {
                    failsafeExecutor = failsafeExecutor.with(this.httpClientConfiguration.getExecutorService());
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
            HttpExchange httpExchange = HttpClient.buildHttpExchange(
                    httpRequest,
                    new HttpResponse(HttpClient.SERVER_ERROR_STATUS_CODE, String.valueOf(exception.getMessage())),
                    Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    attempts,
                    HttpClient.FAILURE);
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
        Optional<Pattern> myRetryResponseCodeRegex = this.httpClientConfiguration.getRetryResponseCodeRegex();
        if (myRetryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = myRetryResponseCodeRegex.get();
            Matcher matcher = retryPattern.matcher("" + httpResponse.getStatusCode());
            return matcher.matches();
        } else {
            return false;
        }
    }

    protected HttpRequest enrich(HttpRequest httpRequest) {
        return this.httpClientConfiguration.getEnrichRequestFunction().apply(httpRequest);
    }


    protected HttpExchange enrichHttpExchange(HttpExchange httpExchange) {
        return this.httpClientConfiguration.getAddSuccessStatusToHttpExchangeFunction().apply(httpExchange);
    }
    @Override
    public boolean matches(HttpRequest httpRequest) {
        return this.httpClientConfiguration.matches(httpRequest);
    }

    @Override
    public C getClient() {
        return this.httpClientConfiguration.getClient();
    }

    public HttpClientConfiguration<C, R, S> getConfiguration() {
        return httpClientConfiguration;
    }
}
