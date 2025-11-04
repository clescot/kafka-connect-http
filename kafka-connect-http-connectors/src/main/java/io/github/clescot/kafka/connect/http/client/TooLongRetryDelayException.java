package io.github.clescot.kafka.connect.http.client;

import java.time.Instant;

public class TooLongRetryDelayException extends RuntimeException {

    private final long secondsToWait;
    private final long retryDelayThreshold;
    private final Instant nextRetryInstant;

    public TooLongRetryDelayException(long secondsToWait, long retryDelayThreshold) {
        super("The retry delay is too long: " + secondsToWait + " seconds. The threshold is " + retryDelayThreshold + " seconds.");
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
}
