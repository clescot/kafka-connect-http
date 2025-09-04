package io.github.clescot.kafka.connect;

import dev.failsafe.RateLimiter;

import java.util.Optional;

public interface Client<E> {


    String getEngineId();

    void setRateLimiter(RateLimiter<E> rateLimiter);

    Optional<RateLimiter<E>> getRateLimiter();

    String getPermitsPerExecution();

}
