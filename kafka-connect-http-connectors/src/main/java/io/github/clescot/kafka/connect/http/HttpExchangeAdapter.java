package io.github.clescot.kafka.connect.http;

import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.time.format.DateTimeFormatter;

import static io.github.clescot.core.http.HttpExchange.*;

public class HttpExchangeAdapter {

    public static final int HTTP_EXCHANGE_VERSION = 2;

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpExchange.class.getName())
            .version(HTTP_EXCHANGE_VERSION)
            //metadata fields
            .field(DURATION_IN_MILLIS_KEY, Schema.INT64_SCHEMA)
            .field(MOMENT_KEY, Schema.STRING_SCHEMA)
            .field(ATTEMPTS_KEY, Schema.INT32_SCHEMA)
            //request
            .field(HTTP_REQUEST_KEY, HttpRequestAdapter.SCHEMA)
            // response
            .field(HTTP_RESPONSE_KEY, HttpResponseAdapter.SCHEMA)
            .field(ATTRIBUTES_KEY, SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).optional().schema())
            .field(TIMINGS_KEY,SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.OPTIONAL_INT64_SCHEMA).optional().schema())
            .schema();
    private final HttpExchange httpExchange;

    private HttpExchangeAdapter(HttpExchange httpExchange) {
        this.httpExchange = httpExchange;
    }

    public static HttpExchangeAdapter from(HttpExchange httpExchange){
        return new HttpExchangeAdapter(httpExchange);
    }

    public Struct toStruct(){
        Struct struct = new Struct(SCHEMA);
        struct.put(DURATION_IN_MILLIS_KEY,httpExchange.getDurationInMillis());
        struct.put(ATTRIBUTES_KEY,httpExchange.getAttributes());
        struct.put(MOMENT_KEY,httpExchange.getMoment().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        struct.put(ATTEMPTS_KEY,httpExchange.getAttempts().intValue());
        //request fields
        struct.put(HTTP_REQUEST_KEY, HttpRequestAdapter.from(httpExchange.getRequest()).toStruct());
        // response fields
        struct.put(HTTP_RESPONSE_KEY, HttpResponseAdapter.from(httpExchange.getResponse()).toStruct());
        struct.put(TIMINGS_KEY,httpExchange.getTimings());
        return struct;

    }

}
