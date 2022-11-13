package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.HttpRequest;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpExchange {


    private final Map<String,String> responseHeaders;
    private final Integer statusCode;
    private final String statusMessage;
    private final String responseBody;
    private final Long durationInMillis;
    private final OffsetDateTime moment;
    private final AtomicInteger attempts;
    private final boolean success;
    private HttpRequest httpRequest;

    public HttpExchange(
            HttpRequest httpRequest,
            Integer statusCode,
            String statusMessage,
            Map<String,String> responseHeaders,
            String responseBody,
            long durationInMillis,
            OffsetDateTime moment,
            AtomicInteger attempts,
            boolean success) {
        this.httpRequest = httpRequest;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.responseHeaders = responseHeaders;
        this.responseBody = responseBody;
        this.durationInMillis = durationInMillis;
        this.moment = moment;
        this.attempts = attempts;
        this.success = success;
    }


    public Map<String,String> getResponseHeaders() {
        return responseHeaders;
    }

    public OffsetDateTime getMoment() {
        return moment;
    }

    public AtomicInteger getAttempts() {
        return attempts;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getResponseBody() {
        return responseBody;
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

    @Override
    public String toString() {
        return "HttpExchange{" +
                "httpRequest=" + httpRequest +
                ", responseHeaders=" + responseHeaders +
                ", statusCode=" + statusCode +
                ", statusMessage='" + statusMessage + '\'' +
                ", responseBody='" + responseBody + '\'' +
                ", durationInMillis='" + durationInMillis + '\'' +
                ", moment='" + moment.format(DateTimeFormatter.ISO_INSTANT) + '\'' +
                ", success='" + success + '\'' +
                ", attempts='" + attempts + '\'' +
                '}';
    }

    public static final class Builder {
        private Map<String,String> responseHeaders;
        private String responseBody;
        private Integer statusCode;
        private String statusMessage;
        private Long durationInMillis;
        private OffsetDateTime moment;
        private AtomicInteger attempts;

        private boolean success;
        private HttpRequest httpRequest;

        private Builder() {
        }

        public static Builder anHttpExchange() {
            return new Builder();
        }


        public Builder withHttpRequest(HttpRequest httpRequest) {
            this.httpRequest = httpRequest;
            return this;
        }


        public Builder withStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public Builder withStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public Builder withResponseBody(String content) {
            this.responseBody = content;
            return this;
        }

        public Builder withSuccess(boolean success) {
            this.success = success;
            return this;
        }

        public HttpExchange build() {
            return new HttpExchange(
                    httpRequest,
                    statusCode,
                    statusMessage,
                    responseHeaders,
                    responseBody,
                    durationInMillis,
                    moment,
                    attempts,
                    success
            );
        }

        public Builder withResponseHeaders(Map<String,String> headers) {
            this.responseHeaders = headers;
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
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpExchange that = (HttpExchange) o;
        return success == that.success && Objects.equals(responseHeaders, that.responseHeaders) && statusCode.equals(that.statusCode) && statusMessage.equals(that.statusMessage) && Objects.equals(responseBody, that.responseBody) && httpRequest.equals(that.httpRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(responseHeaders, statusCode, statusMessage, responseBody, success, httpRequest);
    }
}
