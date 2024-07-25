package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.core.HttpRequest;

import java.util.function.Predicate;

public class Splitter {

    private final String id;
    private final Predicate<HttpRequest> predicate;
    private final String splitPattern;
    private final int splitLimit;

    public Splitter(String id, Predicate<HttpRequest> predicate,String splitPattern,
                    int splitLimit) {
        this.id = id;
        this.predicate = predicate;
        this.splitPattern = splitPattern;
        this.splitLimit = splitLimit;
    }

    public String getId() {
        return id;
    }

    public int getSplitLimit() {
        return splitLimit;
    }

    public String getSplitPattern() {
        return splitPattern;
    }

    public boolean matches(HttpRequest httpRequest) {
        return this.predicate.test(httpRequest);
    }
}
