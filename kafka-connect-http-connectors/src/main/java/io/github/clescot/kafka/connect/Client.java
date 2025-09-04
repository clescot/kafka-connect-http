package io.github.clescot.kafka.connect;

import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.core.HttpExchange;

import java.util.Optional;

public interface Client {

    String getEngineId();

    Optional<RateLimiter<HttpExchange>> getRateLimiter();
}
