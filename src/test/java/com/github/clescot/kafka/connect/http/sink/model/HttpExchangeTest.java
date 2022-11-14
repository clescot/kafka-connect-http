package com.github.clescot.kafka.connect.http.sink.model;


import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpResponse;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.client.HttpClient.SUCCESS;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


public class HttpExchangeTest {

        private HttpRequest getDummyHttpRequest(){
            return new HttpRequest(
                    "http://www.toto.com","GET","STRING","stuff",null,null);
        }
        private HttpResponse getDummyHttpResponse(int statusCode){
            return new HttpResponse(
                    statusCode,"OK","nfgnlksdfnlnskdfnlsf");
        }
        @Test
        public void test_nominal_case() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange httpExchange1 = new HttpExchange(
                    getDummyHttpRequest(),
                  getDummyHttpResponse(200),
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
                   getDummyHttpResponse(statusCode),
                    745L,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS
            );
            assertThat(httpExchange.getHttpResponse().getResponseBody()).isEqualTo(responseBody);
            assertThat(httpExchange.getHttpResponse().getStatusCode()).isEqualTo(statusCode);
        }
}

