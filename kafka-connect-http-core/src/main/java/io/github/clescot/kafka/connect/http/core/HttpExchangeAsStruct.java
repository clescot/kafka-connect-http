package io.github.clescot.kafka.connect.http.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;

public class HttpExchangeAsStruct {

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpExchangeAsStruct.class);
    public static final int HTTP_EXCHANGE_VERSION = 1;
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
            .field(HTTP_REQUEST, HttpRequestAsStruct.SCHEMA)
            // response
            .field(HTTP_RESPONSE, HttpResponseAsStruct.SCHEMA)
            .schema();
    private HttpExchange httpExchange;

    public HttpExchangeAsStruct(HttpExchange httpExchange) {

        this.httpExchange = httpExchange;
    }

    public Struct toStruct(){
        Struct struct = new Struct(SCHEMA);
        struct.put(DURATION_IN_MILLIS,httpExchange.getDurationInMillis());
        struct.put(MOMENT,httpExchange.getMoment().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        struct.put(ATTEMPTS,httpExchange.getAttempts().intValue());
        //request fields
        HttpRequest httpRequest = httpExchange.getHttpRequest();
        HttpRequestAsStruct httpRequestAsStruct = new HttpRequestAsStruct(httpRequest);
        struct.put(HTTP_REQUEST, httpRequestAsStruct.toStruct());
        // response fields
        HttpResponse httpResponse = httpExchange.getHttpResponse();
        HttpResponseAsStruct httpResponseAsStruct = new HttpResponseAsStruct(httpResponse);
        struct.put(HTTP_RESPONSE, httpResponseAsStruct.toStruct());
        return struct;

    }
}
