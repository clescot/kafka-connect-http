package io.github.clescot.kafka.connect.http.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;

public class HttpExchangeAsStruct {

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpExchangeAsStruct.class);



    private final HttpExchange httpExchange;

    public HttpExchangeAsStruct(HttpExchange httpExchange) {

        this.httpExchange = httpExchange;
    }


}
