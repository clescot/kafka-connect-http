package io.github.clescot.kafka.connect.http.sink.client;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.micrometer.common.lang.Nullable;

import java.io.IOException;

// VisibleForTesting
public class CallState {

    final long startTime;

    @Nullable
    final HttpRequest request;

    @Nullable
    HttpResponse response;

    @Nullable
    IOException exception;

    CallState(long startTime, @Nullable HttpRequest request) {
        this.startTime = startTime;
        this.request = request;
    }

}
