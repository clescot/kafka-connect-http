package io.github.clescot.kafka.connect.http;

import io.github.clescot.core.http.HttpExchange;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.HttpResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.client.RequestResponseClient.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;

class HttpExchangeAdapterTest {

    @Nested
    class TestToStruct{
        @Test
        void test_to_struct() {
            OffsetDateTime moment = OffsetDateTime.now(ZoneId.of("UTC"));
            AtomicInteger attempts = new AtomicInteger(2);
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    moment,
                    attempts,
                    SUCCESS);
            HttpExchangeAdapter httpExchangeAdapter = HttpExchangeAdapter.from(httpExchange);
            assertThat(httpExchangeAdapter.toStruct()).isNotNull();
            assertThat(httpExchangeAdapter.toStruct().get("httpRequest")).isNotNull();
            assertThat(httpExchangeAdapter.toStruct().get("httpResponse")).isNotNull();
            assertThat(httpExchangeAdapter.toStruct().get("durationInMillis")).isEqualTo(100L);
            assertThat(httpExchangeAdapter.toStruct().get("moment")).isEqualTo(moment.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            assertThat(httpExchangeAdapter.toStruct().get("attempts")).isEqualTo(attempts.get());
        }
    }

    private HttpRequest getDummyHttpRequest() {
        HttpRequest httpRequest = new HttpRequest(
                "http://www.toto.com", HttpRequest.Method.GET);
        httpRequest.setBodyAsString("stuff");
        return httpRequest;
    }

    private HttpResponse getDummyHttpResponse(int statusCode) {
        HttpResponse httpResponse = new HttpResponse(
                statusCode, "OK");
        httpResponse.setBodyAsString("nfgnlksdfnlnskdfnlsf");
        return httpResponse;
    }

}