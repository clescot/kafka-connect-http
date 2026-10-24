package io.github.clescot.client.http;

import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
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
