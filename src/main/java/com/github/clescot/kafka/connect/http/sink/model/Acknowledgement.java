package com.github.clescot.kafka.connect.http.sink.model;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Acknowledgement {


    private List<Map.Entry<String, String>> requestHeaders;
    private final String method;
    private final String requestBody;
    private List<Map.Entry<String, String>> responseHeaders;
    private String correlationId;
    private Integer statusCode;
    private String statusMessage;
    private String responseBody;
    private long durationInMillis;
    private OffsetDateTime moment;
    private AtomicInteger attempts;
    private final String requestUri;
    private final String wsId;


    public Acknowledgement(
            String correlationId,
            String wsId,
            Integer statusCode,
            String statusMessage,
            List<Map.Entry<String, String>> responseHeaders,
            String responseBody,
            String requestUri,
            List<Map.Entry<String, String>> requestHeaders,
            String method,
            String requestBody,
            long durationInMillis,
            OffsetDateTime moment,
            AtomicInteger attempts) {
        this.correlationId = correlationId;
        this.wsId = wsId;
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
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public List<Map.Entry<String, String>> getRequestHeaders() {
        return requestHeaders;
    }

    public String getMethod() {
        return method;
    }

    public String getRequestBody() {
        return requestBody;
    }

    public List<Map.Entry<String, String>> getResponseHeaders() {
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

    public String getWsId() {
        return wsId;
    }

    public long getDurationInMillis() {
        return durationInMillis;
    }

    @Override
    public String toString() {
        return "Acknowledgement{" +
                "requestHeaders=" + requestHeaders +
                ", method='" + method + '\'' +
                ", requestBody='" + requestBody + '\'' +
                ", responseHeaders=" + responseHeaders +
                ", correlationId='" + correlationId + '\'' +
                ", statusCode=" + statusCode +
                ", statusMessage='" + statusMessage + '\'' +
                ", responseBody='" + responseBody + '\'' +
                ", requestUri='" + requestUri + '\'' +
                ", wsId='" + wsId + '\'' +
                ", durationInMillis='" + durationInMillis + '\'' +
                ", moment='" + moment.format(DateTimeFormatter.ISO_INSTANT) + '\'' +
                ", attempts='" + attempts + '\'' +
                '}';
    }

    public static final class AcknowledgementBuilder {
        private String correlationId;
        private String wsId;
        private String requestUri;
        private String requestMethod;
        private List<Map.Entry<String, String>> requestHeaders;
        private String requestBody;
        private List<Map.Entry<String, String>> responseHeaders;
        private String responseBody;
        private Integer statusCode;
        private String statusMessage;
        private long durationInMillis;
        private OffsetDateTime moment;
        private AtomicInteger attempts;

        private AcknowledgementBuilder() {
        }

        public static AcknowledgementBuilder anAcknowledgement() {
            return new AcknowledgementBuilder();
        }


        public AcknowledgementBuilder withRequestUri(String requestUri) {
            this.requestUri = requestUri;
            return this;
        }

        public AcknowledgementBuilder withWsId(String wsId) {
            this.wsId = wsId;
            return this;
        }


        public AcknowledgementBuilder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public AcknowledgementBuilder withStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public AcknowledgementBuilder withStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public AcknowledgementBuilder withResponseBody(String content) {
            this.responseBody = content;
            return this;
        }

        public Acknowledgement build() {
            return new Acknowledgement(
                    correlationId,
                    wsId,
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
                    attempts
            );
        }

        public AcknowledgementBuilder withRequestBody(String requestBody) {
            this.requestBody = requestBody;
            return this;
        }

        public AcknowledgementBuilder withMethod(String method) {
            this.requestMethod = method;
            return this;
        }

        public AcknowledgementBuilder withRequestHeaders(List<Map.Entry<String, String>> headers) {
            this.requestHeaders = headers;
            return this;
        }

        public AcknowledgementBuilder withResponseHeaders(List<Map.Entry<String, String>> headers) {
            this.responseHeaders = headers;
            return this;
        }

        public AcknowledgementBuilder withDuration(long durationInMillis) {
            this.durationInMillis = durationInMillis;
            return this;
        }

        public AcknowledgementBuilder at(OffsetDateTime moment) {
            this.moment = moment;
            return this;
        }

        public AcknowledgementBuilder withAttempts(AtomicInteger attempts) {
            this.attempts = attempts;
            return this;
        }
    }
}
