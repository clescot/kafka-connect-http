package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;


public class HttpExchange implements Exchange,Cloneable, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public static final int HTTP_EXCHANGE_VERSION = 2;
    public static final String DURATION_IN_MILLIS = "durationInMillis";
    public static final String MOMENT = "moment";
    public static final String ATTEMPTS = "attempts";
    public static final String HTTP_REQUEST = "httpRequest";
    public static final String HTTP_RESPONSE = "httpResponse";
    public static final String ATTRIBUTES = "attributes";
    public static final Schema SCHEMA = SchemaBuilder
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
            .field(ATTRIBUTES, SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).optional().schema())
            .schema();

    private Long durationInMillis;
    private OffsetDateTime moment;
    private AtomicInteger attempts;
    private boolean success;
    private HttpResponse httpResponse;
    private HttpRequest httpRequest;
    @JsonProperty
    private Map<String,String> attributes = Maps.newHashMap();

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
                ", attributes=" + attributes +
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
        struct.put(ATTRIBUTES,this.getAttributes());
        struct.put(MOMENT,this.getMoment().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        struct.put(ATTEMPTS,this.attempts.intValue());
        //request fields
        struct.put(HTTP_REQUEST, this.httpRequest.toStruct());
        // response fields
        struct.put(HTTP_RESPONSE, this.httpResponse.toStruct());
        return struct;

    }

    @Override
    public Object clone() {
        try {
            HttpExchange clone = (HttpExchange) super.clone();
            clone.setAttributes(this.attributes);
            clone.setDurationInMillis(this.durationInMillis);
            clone.setMoment(this.moment);
            clone.setAttempts(new AtomicInteger(this.attempts.get()));
            clone.setHttpResponse((HttpResponse) this.httpResponse.clone());
            clone.setHttpRequest((HttpRequest) this.httpRequest.clone());
            clone.setSuccess(this.success);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public static final class Builder {
        private Long durationInMillis;
        private OffsetDateTime moment;
        private AtomicInteger attempts;

        private boolean success;
        private HttpRequest httpRequest;
        private HttpResponse httpResponse;
        private Map<String, String> attributes;

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

        public Builder withAttributes(Map<String,String> attributes) {
            this.attributes = attributes;
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
            HttpExchange httpExchange = new HttpExchange(
                    httpRequest,
                    httpResponse,
                    durationInMillis,
                    moment,
                    attempts,
                    success
            );
            httpExchange.setAttributes(attributes);
            return httpExchange;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HttpExchange that)) return false;
        return success == that.success
                && Objects.equals(durationInMillis, that.durationInMillis)
                && attributes.equals(that.attributes)
                && Objects.equals(moment, that.moment)
                && Objects.equals(attempts.get(), that.attempts.get())
                && Objects.equals(httpResponse, that.httpResponse)
                && Objects.equals(httpRequest, that.httpRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(durationInMillis,attributes, moment, attempts, success, httpResponse, httpRequest);
    }
}
