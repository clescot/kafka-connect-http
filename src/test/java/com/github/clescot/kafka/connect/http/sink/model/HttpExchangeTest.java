package com.github.clescot.kafka.connect.http.sink.model;


import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.source.HttpExchange;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.client.HttpClient.SUCCESS;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


public class HttpExchangeTest {

        private HttpRequest getDummyHttpRequest(){
            return new HttpRequest(
                    "http://www.toto.com",Maps.newHashMap(),"GET","stuff",null,null);
        }
        @Test
        public void test_nominal_case() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    200,
                    "OK",
                    Maps.newHashMap(),
                    "nfgnlksdfnlnskdfnlsf",
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange httpExchange1 = new HttpExchange(
                    getDummyHttpRequest(),
                    200,
                    "OK",
                    Maps.newHashMap(),
                    "nfgnlksdfnlnskdfnlsf",
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            httpExchange1.equals(httpExchange);
        }

        @Test
        public void test_nominal_case_detail() {
            int statusCode = 404;
            String responseBody = "nfgnlksdfnlnskdfnlsf";
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    statusCode,
                    "Not Found",
                    Maps.newHashMap(),
                    responseBody,
                    745L,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS
            );
            assertThat(httpExchange.getResponseBody()).isEqualTo(responseBody);
            assertThat(httpExchange.getStatusCode()).isEqualTo(statusCode);
        }
}

