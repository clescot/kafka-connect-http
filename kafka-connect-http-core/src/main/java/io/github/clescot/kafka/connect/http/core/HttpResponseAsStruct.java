package io.github.clescot.kafka.connect.http.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class HttpResponseAsStruct {
    private static final Integer VERSION = 2;

    public static final String STATUS_CODE = "statusCode";
    public static final String STATUS_MESSAGE = "statusMessage";
    public static final String PROTOCOL = "protocol";
    public static final String HEADERS = "headers";
    public static final String BODY_AS_STRING = "bodyAsString";

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE,Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE,Schema.STRING_SCHEMA)
            .field(PROTOCOL,Schema.OPTIONAL_STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY_AS_STRING,Schema.OPTIONAL_STRING_SCHEMA);

    private final HttpResponse httpResponse;

    public HttpResponseAsStruct(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(STATUS_CODE,httpResponse.getStatusCode().longValue())
                .put(STATUS_MESSAGE,httpResponse.getStatusMessage())
                .put(HEADERS,httpResponse.getHeaders())
                .put(BODY_AS_STRING,httpResponse.getBodyAsString());


    }
}
