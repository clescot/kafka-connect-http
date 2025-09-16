package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import de.sstoehr.harreader.model.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.core.VersionUtils.VERSION;


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
    public static final String RESPONSE_HEADERS_TIMING_KEY = "responseHeaders";
    public static final String RESPONSE_BODY_TIMING_KEY = "responseBody";
    public static final String DIRECT_ELAPSED_TIME_TIMING_KEY = "directElapsedTime";
    public static final String OVERALL_ELAPSED_TIME_TIMING_KEY = "overallElapsedTime";
    public static final String REQUEST_BODY_TIMING_KEY = "requestBody";
    public static final String REQUEST_HEADERS_TIMING_KEY = "requestHeaders";
    public static final String PROXY_SELECTION_TIMING_KEY = "proxySelection";
    public static final String CONNECTED_TIMING_KEY = "connected";
    public static final String RECEIVE_TIMING_KEY = "receive";
    public static final String WAITING_TIME_TIMING_KEY = "waitingTime";
    public static final String BLOCKED_TIMING_KEY = "blocked";
    public static final String SECURE_CONNECTING_TIMING_KEY = "secureConnecting";
    public static final String CONNECTING_TIMING_KEY = "connecting";
    public static final String DNS_TIMING_KEY = "dns";
    public static final String KAFKA_CONNECT_HTTP = "kafka-connect-http";

    private Long durationInMillis;
    private OffsetDateTime moment;
    private AtomicInteger attempts;
    private boolean success;
    private HttpResponse httpResponse;
    private HttpRequest httpRequest;
    @JsonProperty
    private Map<String,String> attributes = Maps.newHashMap();
    private Map<String,Long> timings = Maps.newHashMap();

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

    public static HttpExchange fromHarEntry(HarEntry harEntry){
        HttpRequest httpRequest = HttpRequest.fromHarRequest(harEntry.request());
        HttpResponse httpResponse = HttpResponse.fromHarResponse(harEntry.response());
        long durationInMillis = harEntry.time().longValue();
        OffsetDateTime moment = harEntry.startedDateTime().toOffsetDateTime();
        AtomicInteger attempts = new AtomicInteger(1);
        boolean success = httpResponse.getStatusCode()>=200 && httpResponse.getStatusCode()<300;
        HttpExchange httpExchange = new HttpExchange(httpRequest,httpResponse,durationInMillis,moment,attempts,success);
        Map<String,Long> timings = Maps.newHashMap();
        if(harEntry.timings()!=null) {
            HarTiming harTiming = harEntry.timings();
            timings.put(DNS_TIMING_KEY, harTiming.dns());
            timings.put(CONNECTING_TIMING_KEY, harTiming.connect());
            timings.put(SECURE_CONNECTING_TIMING_KEY, harTiming.ssl());
            timings.put(REQUEST_HEADERS_TIMING_KEY, harTiming.send());
            timings.put(WAITING_TIME_TIMING_KEY, harTiming.waitTime());
            timings.put(RECEIVE_TIMING_KEY, harTiming.receive());
            if (harTiming.additional() != null) {
                Map<String, Object> additionalTimings = harTiming.additional();
                if (additionalTimings.containsKey(CONNECTED_TIMING_KEY)) {
                    timings.put(CONNECTED_TIMING_KEY, ((Number) additionalTimings.get(CONNECTED_TIMING_KEY)).longValue());
                }
                if (additionalTimings.containsKey(PROXY_SELECTION_TIMING_KEY)) {
                    timings.put(PROXY_SELECTION_TIMING_KEY, ((Number) additionalTimings.get(PROXY_SELECTION_TIMING_KEY)).longValue());
                }
                if (additionalTimings.containsKey(REQUEST_HEADERS_TIMING_KEY)) {
                    timings.put(REQUEST_HEADERS_TIMING_KEY, ((Number) additionalTimings.get(REQUEST_HEADERS_TIMING_KEY)).longValue());
                }
                if (additionalTimings.containsKey(REQUEST_BODY_TIMING_KEY)) {
                    timings.put(REQUEST_BODY_TIMING_KEY, ((Number) additionalTimings.get(REQUEST_BODY_TIMING_KEY)).longValue());
                }
                if (additionalTimings.containsKey(RESPONSE_HEADERS_TIMING_KEY)) {
                    timings.put(RESPONSE_HEADERS_TIMING_KEY, ((Number) additionalTimings.get(RESPONSE_HEADERS_TIMING_KEY)).longValue());
                }
                if (additionalTimings.containsKey(RESPONSE_BODY_TIMING_KEY)) {
                    timings.put(RESPONSE_BODY_TIMING_KEY, ((Number) additionalTimings.get(RESPONSE_BODY_TIMING_KEY)).longValue());
                }
                if (additionalTimings.containsKey(DIRECT_ELAPSED_TIME_TIMING_KEY)) {
                    timings.put(DIRECT_ELAPSED_TIME_TIMING_KEY, ((Number) additionalTimings.get(DIRECT_ELAPSED_TIME_TIMING_KEY)).longValue());
                }
            }
        }
        return httpExchange;
    }

    public HarEntry toHarEntry(){
        return toHarEntry(1,null,null,null);
    }
    public HarEntry toHarEntry(int pageIndex,String connection,String serverIPAddress,String comment){
        HarEntry.HarEntryBuilder harEntryBuilder = HarEntry.builder();
        harEntryBuilder.time(this.getDurationInMillis().intValue());
        harEntryBuilder.request(this.getHttpRequest().toHarRequest(this.getHttpResponse().getProtocol()));
        harEntryBuilder.response(this.getHttpResponse().toHarResponse());
        if(!Strings.isNullOrEmpty(comment)) {
            harEntryBuilder.comment(comment);
        }
        //harEntryBuilder.additional()
        if(!Strings.isNullOrEmpty(connection)) {
            harEntryBuilder.connection(connection);
        }
        //harEntryBuilder.cache()
        harEntryBuilder.pageref("page_"+pageIndex);
        if(!Strings.isNullOrEmpty(serverIPAddress)){
            harEntryBuilder.serverIPAddress(serverIPAddress);
        }
        HarTiming harTiming = getHarTiming();
        harEntryBuilder.timings(harTiming);
        return harEntryBuilder.build();
    }

    private HarTiming getHarTiming() {
        HarTiming.HarTimingBuilder harTimingBuilder = HarTiming.builder();
        Long dns = timings.get(DNS_TIMING_KEY);
        if (dns != null) {
            harTimingBuilder.dns(dns);
        }
        Long connecting = timings.get(CONNECTING_TIMING_KEY);
        if(connecting!=null) {
            harTimingBuilder.connect(connecting);
        }
        Long ssl = timings.get(SECURE_CONNECTING_TIMING_KEY);
        if(ssl!=null) {
            harTimingBuilder.ssl(ssl);
        }

        Long requestHeaders = timings.get(REQUEST_HEADERS_TIMING_KEY);
        Long requestBody = timings.get(REQUEST_BODY_TIMING_KEY);
        harTimingBuilder.send((requestHeaders!=null?requestHeaders:0)+(requestBody!=null?requestBody:0));
        Long blocked = timings.get(BLOCKED_TIMING_KEY);
        if(blocked==null){
            blocked=0L;
        }
        harTimingBuilder.blocked(blocked);
        Long waitTime = timings.get(WAITING_TIME_TIMING_KEY);
        if(waitTime!=null) {
            harTimingBuilder.waitTime(waitTime);
        }
        Long receive = timings.get(RECEIVE_TIMING_KEY);
        if(receive!=null) {
            harTimingBuilder.receive(receive);
        }
        Map<String,Object> additionalTimings = Maps.newHashMap();
        additionalTimings.put(CONNECTED_TIMING_KEY,timings.get(CONNECTED_TIMING_KEY));
        additionalTimings.put(PROXY_SELECTION_TIMING_KEY,timings.get(PROXY_SELECTION_TIMING_KEY));
        additionalTimings.put(REQUEST_HEADERS_TIMING_KEY, requestHeaders);
        additionalTimings.put(REQUEST_BODY_TIMING_KEY, requestBody);
        additionalTimings.put(RESPONSE_HEADERS_TIMING_KEY,timings.get(RESPONSE_HEADERS_TIMING_KEY));
        additionalTimings.put(RESPONSE_BODY_TIMING_KEY,timings.get(RESPONSE_BODY_TIMING_KEY));
        additionalTimings.put(DIRECT_ELAPSED_TIME_TIMING_KEY,timings.get(DIRECT_ELAPSED_TIME_TIMING_KEY));
        additionalTimings.put(OVERALL_ELAPSED_TIME_TIMING_KEY,timings.get(OVERALL_ELAPSED_TIME_TIMING_KEY));

        harTimingBuilder.additional(additionalTimings);
        HarTiming harTiming = harTimingBuilder.build();
        return harTiming;
    }


    public static Har toHar(HttpExchange... exchanges){

        Har.HarBuilder harBuilder = Har.builder();
        HarLog.HarLogBuilder harLogBuilder = HarLog.builder();
        harLogBuilder.version("1.2");

        //browser
        HarCreatorBrowser.HarCreatorBrowserBuilder creatorBrowserBuilder = HarCreatorBrowser.builder();
        creatorBrowserBuilder.name(KAFKA_CONNECT_HTTP);
        creatorBrowserBuilder.version(VERSION);
        harLogBuilder.browser(creatorBrowserBuilder.build());



        //creator
        HarCreatorBrowser.HarCreatorBrowserBuilder creatorBuilder = HarCreatorBrowser.builder();
        creatorBuilder.name(KAFKA_CONNECT_HTTP);
        creatorBuilder.version(VERSION);
        harLogBuilder.creator(creatorBuilder.build());
        for (int i=0;i<exchanges.length;i++) {
            HttpExchange exchange = exchanges[i];
            harLogBuilder.entry(exchange.toHarEntry(i+1,null,null,null));
            HarPage.HarPageBuilder harPageBuilder = HarPage.builder();
            harPageBuilder.id("page_" + (i + 1));
            //harPageBuilder.title("page_" + (i + 1));
            harPageBuilder.startedDateTime(ZonedDateTime.ofInstant(exchange.getMoment().toInstant(), exchange.getMoment().getOffset()));
//          HarPageTiming.HarPageTimingBuilder harPageTimingBuilder = HarPageTiming.builder();
//          harPageTimingBuilder.onContentLoad(0);
//          harPageTimingBuilder.onLoad(0);
            HarPage harPage = harPageBuilder.build();
            harLogBuilder.page(harPage);
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
