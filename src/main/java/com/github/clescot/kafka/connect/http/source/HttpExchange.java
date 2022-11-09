package com.github.clescot.kafka.connect.http.source;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpExchange {


    private final Map<String,String> requestHeaders;
    private final String method;
    private final String requestBody;
    private final Map<String,String> responseHeaders;
    private final String correlationId;
    private final Integer statusCode;
    private final String statusMessage;
    private final String responseBody;
    private final long durationInMillis;
    private final OffsetDateTime moment;
    private final AtomicInteger attempts;
    private final String requestUri;
    private final String requestId;
    private final boolean success;

    public HttpExchange(
            String correlationId,
            String requestId,
            Integer statusCode,
            String statusMessage,
            Map<String,String> responseHeaders,
            String responseBody,
            String requestUri,
            Map<String,String> requestHeaders,
            String method,
            String requestBody,
            long durationInMillis,
            OffsetDateTime moment,
            AtomicInteger attempts,
            boolean success) {
        this.correlationId = correlationId;
        this.requestId = requestId;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.requestUri = requestUri;
        this.requestHeaders = requestHeaders;
        this.requestBody = requestBody;
        this.method = method;
        this.responseHeaders = responseHeaders;
        this.responseBody = responseBody;
        this.durationInMillis = durationInMillis;
        this.moment = moment;
        this.attempts = attempts;
        this.success = success;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Map<String,String> getRequestHeaders() {
        return requestHeaders;
    }

    public String getMethod() {
        return method;
    }

    public String getRequestBody() {
        return requestBody;
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

    public String getRequestUri() {
        return requestUri;
    }

    public String getRequestId() {
        return requestId;
    }

    public long getDurationInMillis() {
        return durationInMillis;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "HttpExchange{" +
                "requestHeaders=" + requestHeaders +
                ", method='" + method + '\'' +
                ", requestBody='" + requestBody + '\'' +
                ", responseHeaders=" + responseHeaders +
                ", correlationId='" + correlationId + '\'' +
                ", statusCode=" + statusCode +
                ", statusMessage='" + statusMessage + '\'' +
                ", responseBody='" + responseBody + '\'' +
                ", requestUri='" + requestUri + '\'' +
                ", requestId='" + requestId + '\'' +
                ", durationInMillis='" + durationInMillis + '\'' +
                ", moment='" + moment.format(DateTimeFormatter.ISO_INSTANT) + '\'' +
                ", success='" + success + '\'' +
                ", attempts='" + attempts + '\'' +
                '}';
    }

    public static final class Builder {
        private String correlationId;
        private String requestId;
        private String requestUri;
        private String requestMethod;
        private Map<String,String> requestHeaders;
        private String requestBody;
        private Map<String,String> responseHeaders;
        private String responseBody;
        private Integer statusCode;
        private String statusMessage;
        private long durationInMillis;
        private OffsetDateTime moment;
        private AtomicInteger attempts;

        private boolean success;

        private Builder() {
        }

        public static Builder anHttpExchange() {
            return new Builder();
        }


        public Builder withRequestUri(String requestUri) {
            this.requestUri = requestUri;
            return this;
        }

        public Builder withRequestId(String requestId) {
            this.requestId = requestId;
            return this;
        }


        public Builder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
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
                    correlationId,
                    requestId,
                    statusCode,
                    statusMessage,
                    responseHeaders,
                    responseBody,
                    requestUri,
                    requestHeaders,
                    requestMethod,
                    requestBody,
                    durationInMillis,
                    moment,
                    attempts,
                    success
            );
        }

        public Builder withRequestBody(String requestBody) {
            this.requestBody = requestBody;
            return this;
        }

        public Builder withMethod(String method) {
            this.requestMethod = method;
            return this;
        }

        public Builder withRequestHeaders(Map<String,String> headers) {
            this.requestHeaders = headers;
            return this;
        }

        public Builder withResponseHeaders(Map<String,String> headers) {
            this.responseHeaders = headers;
            return this;
        }

        public Builder withDuration(long durationInMillis) {
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
}
