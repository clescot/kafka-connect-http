package com.github.clescot.kafka.connect.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.List;
import java.util.Map;

public class HttpResponse {

    private static final Integer VERSION = 1;

    private static final String STATUS_CODE = "statusCode";
    private static final String STATUS_MESSAGE = "statusMessage";
    private static final String HEADERS = "headers";
    private static final String BODY = "body";

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE,Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE,Schema.STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY,Schema.STRING_SCHEMA);


    private final Integer statusCode;
    private final String statusMessage;
    private final String responseBody;

    private Map<String, List<String>> responseHeaders = Maps.newHashMap();

    public HttpResponse(Integer statusCode, String statusMessage, String responseBody) {
        Preconditions.checkArgument(statusCode>0,"status code must be a positive integer");
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.responseBody = responseBody;
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



    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(STATUS_CODE,statusCode.longValue())
                .put(STATUS_MESSAGE,statusMessage)
                .put(HEADERS,responseHeaders)
                .put(BODY,responseBody);


    }
}
