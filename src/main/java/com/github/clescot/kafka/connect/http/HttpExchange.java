package com.github.clescot.kafka.connect.http;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpExchange {
    public static final int HTTP_EXCHANGE_VERSION = 1;
    public static final String DURATION_IN_MILLIS = "durationInMillis";
    public static final String MOMENT = "moment";
    public static final String ATTEMPTS = "attempts";
    public static final String REQUEST = "request";
    public static final String RESPONSE = "response";
    public final static Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpExchange.class.getName())
            .version(HTTP_EXCHANGE_VERSION)
            //metadata fields
            .field(DURATION_IN_MILLIS, Schema.INT64_SCHEMA)
            .field(MOMENT, Schema.STRING_SCHEMA)
            .field(ATTEMPTS, Schema.INT32_SCHEMA)
            //request
            .field(REQUEST, HttpRequest.SCHEMA)
            // response
            .field(RESPONSE, HttpResponse.SCHEMA)
            .schema();;
    private final Long durationInMillis;
    private final OffsetDateTime moment;
    private final AtomicInteger attempts;
    private final boolean success;
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
