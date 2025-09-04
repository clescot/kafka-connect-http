package io.github.clescot.kafka.connect;

import dev.failsafe.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public interface Client<E> {
    Logger LOGGER = LoggerFactory.getLogger(Client.class);


    String getEngineId();

    void setRateLimiter(RateLimiter<E> rateLimiter);


    Optional<RateLimiter<E>> getRateLimiter();




}
