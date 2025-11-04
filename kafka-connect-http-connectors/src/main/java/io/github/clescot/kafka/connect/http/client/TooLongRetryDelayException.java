package io.github.clescot.kafka.connect.http.client;

public class TooLongRetryDelayException extends RuntimeException {

    private final long secondsToWait;
    private final long retryDelayThreshold;

    public TooLongRetryDelayException(long secondsToWait, long retryDelayThreshold) {
        super("The retry delay is too long: " + secondsToWait + " seconds. The threshold is " + retryDelayThreshold + " seconds.");
        this.secondsToWait = secondsToWait;
        this.retryDelayThreshold = retryDelayThreshold;
    }


    public long getSecondsToWait() {
        return secondsToWait;
    }

    public long getRetryDelayThreshold() {
        return retryDelayThreshold;
    }
}
