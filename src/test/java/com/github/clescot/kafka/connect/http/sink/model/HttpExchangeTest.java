package com.github.clescot.kafka.connect.http.sink.model;


import com.github.clescot.kafka.connect.http.source.HttpExchange;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.client.HttpClient.SUCCESS;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


@RunWith(Enclosed.class)
public class HttpExchangeTest {
    public static class TestHttpExchange {

        @Test
        public void test_nominal_case() {
            HttpExchange httpExchange = new HttpExchange(
                    "dfsdfsd",
                    "sd897osdmsdg",
                    200,
                    "toto",
                    Maps.newHashMap(),
                    "nfgnlksdfnlnskdfnlsf",
                    "http://toto:8081",
                    Maps.newHashMap(),
                    "PUT",
                    "",
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange httpExchange1 = new HttpExchange("dfsdfsd",
                    "sd897osdmsdg",
                    200,
                    "toto",
                    Maps.newHashMap(),
                    "nfgnlksdfnlnskdfnlsf",
                    "http://toto:8081",
                    Maps.newHashMap(),
                    "PUT",
                    "",
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            httpExchange1.equals(httpExchange);
        }

        @Test
        public void test_nominal_case_detail() {
            HttpExchange httpExchange = new HttpExchange(
                    "sdfsfsdf5555",
                    "sd897osdmsdg",
                    200,
                    "toto",
                    Maps.newHashMap(),
                    "nfgnlksdfnlnskdfnlsf",
                    "http://toto:8081",
                    Maps.newHashMap(),
                    "PUT",
                    "",
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS
            );
            assertThat(httpExchange.getResponseBody()).isEqualTo("nfgnlksdfnlnskdfnlsf");
            assertThat(httpExchange.getCorrelationId()).isEqualTo("sdfsfsdf5555");
            assertThat(httpExchange.getStatusCode()).isEqualTo(200);
        }
    }
}

