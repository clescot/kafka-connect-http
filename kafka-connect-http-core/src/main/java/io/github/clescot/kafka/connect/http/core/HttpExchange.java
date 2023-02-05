package io.github.clescot.kafka.connect.http.core;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpExchange.SCHEMA_AS_STRING,refs = {})
public class HttpExchange implements Serializable {

    public static final long serialVersionUID = 1L;
    public static final String BASE_SCHEMA_ID = "https://raw.githubusercontent.com/clescot/kafka-connect-http/master/kafka-connect-http-core/src/main/resources/schemas/";
    public static final String SCHEMA_ID = BASE_SCHEMA_ID+"http-exchange.json";
    public static final String SCHEMA_AS_STRING = "{\n" +
            "  \"$id\": \"" + SCHEMA_ID + "\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema\",\n" +
            "  \"title\": \"Http Exchange\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"javaType\" : \"com.github.clescot.kafka.connect.http.HttpExchange\",\n" +
            "  \"additionalProperties\": false,\n" +
            "  \"properties\": {\n" +
            "    \"durationInMillis\": {\n" +
            "      \"type\": \"integer\"\n" +
            "    },\n" +
            "    \"moment\": {\n" +
            "      \"type\": \"number\"\n" +
            "    },\n" +
            "    \"attempts\": {\n" +
            "      \"type\": \"integer\"\n" +
            "    },\n" +
            "    \"success\": {\n" +
            "      \"type\": \"boolean\"\n" +
            "    },\n" +
            "    \"httpResponse\": {\n" +
            "      \"$ref\": \"./http-response.json\"\n" +
            "    },\n" +
            "    \"httpRequest\": {\n" +
            "      \"$ref\": \"./http-request.json\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"moment\",\n" +
            "    \"attempts\",\n" +
            "    \"success\",\n" +
            "    \"httpRequest\",\n" +
            "    \"httpResponse\"\n" +
            "  ]\n" +
            "\n" +
            "}";

    private Long durationInMillis;
    private OffsetDateTime moment;
    private AtomicInteger attempts;
    private boolean success;
    private HttpResponse httpResponse;
    private HttpRequest httpRequest;

    protected HttpExchange() {
    }

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

    protected void setDurationInMillis(Long durationInMillis) {
        this.durationInMillis = durationInMillis;
    }

    protected void setMoment(OffsetDateTime moment) {
        this.moment = moment;
    }

    protected void setAttempts(AtomicInteger attempts) {
        this.attempts = attempts;
    }

    protected void setHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    protected void setHttpRequest(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
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
