package io.github.clescot.kafka.connect.http.client;

import io.github.clescot.kafka.connect.http.core.HttpExchange;

import java.time.Instant;

public class TooLongRetryDelayException extends RuntimeException {

    private final HttpExchange httpExchange;
    private final long secondsToWait;
    private final long retryDelayThreshold;
    private final Instant nextRetryInstant;

    public TooLongRetryDelayException(HttpExchange httpExchange,
                                      long secondsToWait,
                                      long retryDelayThreshold){
        super("The retry delay is too long: " + secondsToWait + " seconds. The threshold is " + retryDelayThreshold + " seconds.");
        this.httpExchange = httpExchange;
        this.secondsToWait = secondsToWait;
        nextRetryInstant = Instant.now().plusSeconds(secondsToWait);
        this.retryDelayThreshold = retryDelayThreshold;
    }


    public long getSecondsToWait() {
        return secondsToWait;
    }
    public Instant getNextRetryInstant() {
        return nextRetryInstant;
    }

    public long getRetryDelayThreshold() {
        return retryDelayThreshold;
    }

    public HttpExchange getHttpExchange() {
        return httpExchange;
    }
}
