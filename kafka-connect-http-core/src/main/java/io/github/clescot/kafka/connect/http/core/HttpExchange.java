package io.github.clescot.kafka.connect.http.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;


public class HttpExchange implements Cloneable, Serializable {

    public static final long serialVersionUID = 1L;
    public static final int VERSION = 2;
    public static final int HTTP_EXCHANGE_VERSION = 2;
    public static final String DURATION_IN_MILLIS = "durationInMillis";
    public static final String MOMENT = "moment";
    public static final String ATTEMPTS = "attempts";
    public static final String HTTP_REQUEST = "httpRequest";
    public static final String HTTP_RESPONSE = "httpResponse";
    public final static Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpExchange.class.getName())
            .version(HTTP_EXCHANGE_VERSION)
            //metadata fields
            .field(DURATION_IN_MILLIS, Schema.INT64_SCHEMA)
            .field(MOMENT, Schema.STRING_SCHEMA)
            .field(ATTEMPTS, Schema.INT32_SCHEMA)
            //request
            .field(HTTP_REQUEST, HttpRequest.SCHEMA)
            // response
            .field(HTTP_RESPONSE, HttpResponse.SCHEMA)
            .schema();
    public static final String BASE_SCHEMA_ID = "https://raw.githubusercontent.com/clescot/kafka-connect-http/master/kafka-connect-http-core/src/main/resources/schemas/json/versions/";


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

    public Struct toStruct(){
        Struct struct = new Struct(SCHEMA);
        struct.put(DURATION_IN_MILLIS,this.getDurationInMillis());
        struct.put(MOMENT,this.getMoment().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        struct.put(ATTEMPTS,this.getAttempts().intValue());
        //request fields
        HttpRequest httpRequest = this.getHttpRequest();
        struct.put(HTTP_REQUEST, httpRequest.toStruct());
        // response fields
        HttpResponse httpResponse = this.getHttpResponse();
        struct.put(HTTP_RESPONSE, httpResponse.toStruct());
        return struct;

    }

    @Override
    public HttpExchange clone() {
        try {
            HttpExchange clone = (HttpExchange) super.clone();
            clone.setDurationInMillis(this.durationInMillis);
            clone.setMoment(this.moment);
            clone.setAttempts(new AtomicInteger(this.attempts.get()));
            clone.setHttpResponse(this.httpResponse.clone());
            clone.setHttpRequest(this.httpRequest.clone());
            clone.setSuccess(this.success);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HttpExchange)) return false;
        HttpExchange that = (HttpExchange) o;
        return success == that.success && Objects.equals(durationInMillis, that.durationInMillis) && Objects.equals(moment, that.moment) && Objects.equals(attempts.get(), that.attempts.get()) && Objects.equals(httpResponse, that.httpResponse) && Objects.equals(httpRequest, that.httpRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(durationInMillis, moment, attempts, success, httpResponse, httpRequest);
    }
}
