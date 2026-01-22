package io.github.clescot.kafka.connect.http;

import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpPart;
import io.github.clescot.core.http.HttpResponse;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;


import java.util.Base64;

import static io.github.clescot.core.http.HttpResponse.*;

public class HttpResponseAdapter {

    public static final int HTTP_EXCHANGE_VERSION = 2;

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE_FIELD, Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE_FIELD, Schema.STRING_SCHEMA)
            .field(PROTOCOL_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(HEADERS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY_TYPE_FIELD, Schema.STRING_SCHEMA)
            .field(BODY_AS_BYTE_ARRAY_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_STRING_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PARTS_FIELD, SchemaBuilder.array(HttpPart.SCHEMA).optional().schema())
            .field(ATTRIBUTES_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .schema();
    private final HttpResponse httpResponse;

    private HttpResponseAdapter(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    public static HttpResponseAdapter from(HttpResponse httpResponse){
        return new HttpResponseAdapter(httpResponse);
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(STATUS_CODE_FIELD, httpResponse.getStatusCode().longValue())
                .put(STATUS_MESSAGE_FIELD, httpResponse.getStatusMessage())
                .put(PROTOCOL_FIELD, httpResponse.getProtocol())
                .put(HEADERS_FIELD, httpResponse.getHeaders())
                .put(BODY_TYPE_FIELD, httpResponse.getBodyType().toString())
                .put(BODY_AS_BYTE_ARRAY_FIELD, Base64.getEncoder().encodeToString(httpResponse.getBodyAsByteArray()))
                .put(BODY_AS_STRING_FIELD, httpResponse.getBodyAsString())
                .put(ATTRIBUTES_FIELD, httpResponse.getAttributes())
                ;
    }

}
