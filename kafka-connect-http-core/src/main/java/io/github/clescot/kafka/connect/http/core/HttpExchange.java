package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import de.sstoehr.harreader.model.*;
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
    public static final String DURATION_IN_MILLIS_KEY = "durationInMillis";
    public static final String MOMENT_KEY = "moment";
    public static final String ATTEMPTS_KEY = "attempts";
    public static final String HTTP_REQUEST_KEY = "httpRequest";
    public static final String HTTP_RESPONSE_KEY = "httpResponse";
    public static final String ATTRIBUTES_KEY = "attributes";
    private static final String TIMINGS_KEY = "timings";
    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpExchange.class.getName())
            .version(HTTP_EXCHANGE_VERSION)
            //metadata fields
            .field(DURATION_IN_MILLIS_KEY, Schema.INT64_SCHEMA)
            .field(MOMENT_KEY, Schema.STRING_SCHEMA)
            .field(ATTEMPTS_KEY, Schema.INT32_SCHEMA)
            //request
            .field(HTTP_REQUEST_KEY, HttpRequest.SCHEMA)
            // response
            .field(HTTP_RESPONSE_KEY, HttpResponse.SCHEMA)
            .field(ATTRIBUTES_KEY, SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).optional().schema())
            .field(TIMINGS_KEY,SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.OPTIONAL_INT64_SCHEMA).optional().schema())
            .schema();

    private Long durationInMillis;
    private OffsetDateTime moment;
    private AtomicInteger attempts;
    private boolean success;
    private HttpResponse httpResponse;
    private HttpRequest httpRequest;
    @JsonProperty
    private Map<String,String> attributes = Maps.newHashMap();
    private Map<String,Long> timings = Maps.newHashMap();
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private static final String VERSION = VERSION_UTILS.getVersion();

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

    public Map<String, Long> getTimings() {
        return timings;
    }

    public void setTimings(Map<String, Long> timings) {
        this.timings = timings;
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
                ", timings=" + timings +
                '}';
    }

    public Struct toStruct(){
        Struct struct = new Struct(SCHEMA);
        struct.put(DURATION_IN_MILLIS_KEY,this.getDurationInMillis());
        struct.put(ATTRIBUTES_KEY,this.getAttributes());
        struct.put(MOMENT_KEY,this.getMoment().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        struct.put(ATTEMPTS_KEY,this.attempts.intValue());
        //request fields
        struct.put(HTTP_REQUEST_KEY, this.httpRequest.toStruct());
        // response fields
        struct.put(HTTP_RESPONSE_KEY, this.httpResponse.toStruct());
        struct.put(TIMINGS_KEY,this.getTimings());
        return struct;

    }
    public HarEntry toHarEntry(){
        HarEntry harEntry = new HarEntry();
        HarEntry.HarEntryBuilder harEntryBuilder = HarEntry.builder();
        harEntryBuilder.time(this.getDurationInMillis().intValue());
        harEntryBuilder.request(this.getHttpRequest().toHarRequest(this.getHttpResponse().getProtocol()));
        harEntryBuilder.response(this.getHttpResponse().toHarResponse());
        //harEntryBuilder.comment()
        //harEntryBuilder.additional()
        //harEntryBuilder.connection()
        //harEntryBuilder.cache()
        //harEntryBuilder.pageref()
        //harEntryBuilder.serverIPAddress()
        HarTiming.HarTimingBuilder harTimingBuilder = HarTiming.builder();
        harTimingBuilder.dns(timings.get("dns"));
        harTimingBuilder.connect(timings.get("connect"));
        harTimingBuilder.ssl(timings.get("ssl"));
        harTimingBuilder.send(timings.get("send"));
        harTimingBuilder.blocked(timings.get("blocked"));
        harTimingBuilder.waitTime(timings.get("wait"));
        harTimingBuilder.receive(timings.get("receive"));
        Map<String,Object> additionalTimings = Maps.newHashMap();
        harTimingBuilder.additional(additionalTimings);
        harEntryBuilder.timings(harTimingBuilder.build());
        //harEntryBuilder.timings()
        return harEntry;
    }


    public static Har toHar(HttpExchange... exchanges){

        Har.HarBuilder harBuilder = Har.builder();
        HarLog.HarLogBuilder harLogBuilder = HarLog.builder();
        harLogBuilder.version("1.2");

        //browser
        HarCreatorBrowser.HarCreatorBrowserBuilder creatorBrowserBuilder = HarCreatorBrowser.builder();
        creatorBrowserBuilder.name("kafka-connect-http");
        creatorBrowserBuilder.version(VERSION);
        harLogBuilder.browser(creatorBrowserBuilder.build());

        //creator
        HarCreatorBrowser.HarCreatorBrowserBuilder creatorBuilder = HarCreatorBrowser.builder();
        creatorBuilder.name("kafka-connect-http");
        creatorBuilder.version(VERSION);
        harLogBuilder.creator(creatorBuilder.build());
        for (HttpExchange exchange : exchanges) {
            harLogBuilder.entry(exchange.toHarEntry());
        }
        harBuilder.log(harLogBuilder.build());

        return harBuilder.build();
    }

    @Override
    public Object clone() {
            HttpExchange clone = new HttpExchange(httpRequest, httpResponse, durationInMillis, moment, attempts, success);
            clone.setAttributes(this.attributes);
            clone.setTimings(this.timings);
            return clone;
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
        private Map<String, String> attributes = Maps.newHashMap();
        private Map<String, Long> timings = Maps.newHashMap();

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

        public Builder withAttribute(String key,String value) {
            this.attributes.put(key, value);
            return this;
        }

        public Builder withAttributes(Map<String,String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder withTimings(Map<String,Long> timings) {
            this.timings = timings;
            return this;
        }

        public Builder withTiming(String key,long value) {
            this.timings.put(key, value);
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
            httpExchange.setTimings(timings);
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
