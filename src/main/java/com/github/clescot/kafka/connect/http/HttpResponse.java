package com.github.clescot.kafka.connect.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HttpResponse {

    private static final Integer VERSION = 1;

    private static final String STATUS_CODE = "statusCode";
    private static final String STATUS_MESSAGE = "statusMessage";
    private static final String PROTOCOL = "protocol";
    private static final String HEADERS = "headers";
    private static final String BODY = "body";

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE,Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE,Schema.STRING_SCHEMA)
            .field(PROTOCOL,Schema.OPTIONAL_STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY,Schema.OPTIONAL_STRING_SCHEMA);


    private final Integer statusCode;
    private final String statusMessage;
    private String responseBody;
    private String protocol;

    private Map<String, List<String>> responseHeaders = Maps.newHashMap();

    public HttpResponse(Integer statusCode, String statusMessage) {
        Preconditions.checkArgument(statusCode>0,"status code must be a positive integer");
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
    }

    public Map<String, List<String>> getResponseHeaders() {
        return responseHeaders;
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

    public void setResponseHeaders(Map<String, List<String>> responseHeaders) {
        this.responseHeaders = responseHeaders;
    }

    public void setResponseBody(String responseBody) {
        this.responseBody = responseBody;
    }


    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(STATUS_CODE,statusCode.longValue())
                .put(STATUS_MESSAGE,statusMessage)
                .put(HEADERS,responseHeaders)
                .put(BODY,responseBody);


    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpResponse that = (HttpResponse) o;
        return statusCode.equals(that.statusCode) && statusMessage.equals(that.statusMessage) && protocol.equals(that.protocol)&& responseBody.equals(that.responseBody) && Objects.equals(responseHeaders, that.responseHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusCode, statusMessage, responseBody, responseHeaders);
    }

    @Override
    public String toString() {
        return "HttpResponse{" +
                "statusCode=" + statusCode +
                ", statusMessage='" + statusMessage + '\'' +
                ", protocol='" + protocol + '\'' +
                ", responseBody='" + responseBody + '\'' +
                ", responseHeaders=" + responseHeaders +
                '}';
    }
}
