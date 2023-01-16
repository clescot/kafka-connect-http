package com.github.clescot.kafka.connect.http;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpExchange {


    private final Long durationInMillis;
    private final OffsetDateTime moment;
    private final AtomicInteger attempts;
    private boolean success;
    private final HttpResponse httpResponse;
    private HttpRequest httpRequest;

    public HttpExchange(
            HttpRequest httpRequest,
            HttpResponse httpResponse,
            long durationInMillis,
            OffsetDateTime moment,
            AtomicInteger attempts,
            boolean success) {
        this.httpRequest = httpRequest;
        this.httpResponse = httpResponse;
        this.durationInMillis = durationInMillis;
        this.moment = moment;
        this.attempts = attempts;
        this.success = success;
    }


    public OffsetDateTime getMoment() {
        return moment;
    }

    public AtomicInteger getAttempts() {
        return attempts;
    }

    public Long getDurationInMillis() {
        return durationInMillis;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }


    @Override
    public String toString() {
        return "HttpExchange{" +
                "durationInMillis=" + durationInMillis +
                ", moment=" + moment +
                ", attempts=" + attempts +
                ", success=" + success +
                ", httpRequest=" + httpRequest +
                ", httpResponse=" + httpResponse +
                '}';
    }


    public static final class Builder {
        private Long durationInMillis;
        private OffsetDateTime moment;
        private AtomicInteger attempts;

        private boolean success;
        private HttpRequest httpRequest;
        private HttpResponse httpResponse;

        private Builder() {
        }

        public static Builder anHttpExchange() {
            return new Builder();
        }


        public Builder withHttpRequest(HttpRequest httpRequest) {
            this.httpRequest = httpRequest;
            return this;
        }

        public Builder withHttpResponse(HttpResponse httpResponse) {
            this.httpResponse = httpResponse;
            return this;
        }


        public Builder withSuccess(boolean success) {
            this.success = success;
            return this;
        }


        public Builder withDuration(Long durationInMillis) {
            this.durationInMillis = durationInMillis;
            return this;
        }

        public Builder at(OffsetDateTime moment) {
            this.moment = moment;
            return this;
        }

        public Builder withAttempts(AtomicInteger attempts) {
            this.attempts = attempts;
            return this;
        }

        public HttpExchange build() {
            return new HttpExchange(
                    httpRequest,
                    httpResponse,
                    durationInMillis,
                    moment,
                    attempts,
                    success
            );
        }
    }


}
