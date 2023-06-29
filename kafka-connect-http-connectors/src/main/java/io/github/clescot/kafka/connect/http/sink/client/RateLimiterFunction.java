package io.github.clescot.kafka.connect.http.sink.client;

import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class RateLimiterFunction<Req,Res> implements Function<Req, CompletableFuture<Res>> {
    Logger LOGGER = LoggerFactory.getLogger(RateLimiterFunction.class);
    private final RateLimiter<HttpExchange> rateLimiter;
    private final HttpClient<Req, Res> httpClient;

    public RateLimiterFunction(RateLimiter<HttpExchange> rateLimiter,HttpClient<Req,Res> httpClient) {
        this.rateLimiter = rateLimiter;
        this.httpClient = httpClient;
    }

    @Override
    public CompletableFuture<Res> apply(Req req) {
        try {
            Optional<RateLimiter<HttpExchange>> rateLimiter = Optional.ofNullable(this.rateLimiter);
            if (rateLimiter.isPresent()) {
                rateLimiter.get().acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                LOGGER.debug("permits acquired request:'{}'", req);
            }
            return httpClient.nativeCall(req);
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire execution permit from the rate limiter {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        }
    }
}
