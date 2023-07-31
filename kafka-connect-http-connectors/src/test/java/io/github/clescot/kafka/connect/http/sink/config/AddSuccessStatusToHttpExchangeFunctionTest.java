package io.github.clescot.kafka.connect.http.sink.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.Configuration;
import io.github.clescot.kafka.connect.http.sink.HttpSinkConnectorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.jmx.JmxMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.SUCCESS_RESPONSE_CODE_REGEX;
import static org.assertj.core.api.Assertions.assertThat;

class AddSuccessStatusToHttpExchangeFunctionTest {
    private static final String DUMMY_METHOD = "POST";
    private static final String DUMMY_BODY_TYPE = "STRING";

    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Test
    public void test_is_success_with_200() {

        Map<String, String> config = Maps.newHashMap();
        config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
        Configuration configuration = new Configuration("dummy", new HttpSinkConnectorConfig(config), executorService, new JmxMeterRegistry(s -> null, Clock.SYSTEM));
        HttpExchange httpExchange = getDummyHttpExchange();
        boolean success = configuration.enrich(httpExchange).isSuccess();
        assertThat(success).isTrue();
    }

    @Test
    public void test_is_not_success_with_200_by_configuration() {
        Map<String, String> config = Maps.newHashMap();
        config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^1[0-9][0-9]$");
        Configuration configuration = new Configuration("dummy", new HttpSinkConnectorConfig(config), executorService, new JmxMeterRegistry(s -> null, Clock.SYSTEM));
        HttpExchange httpExchange = getDummyHttpExchange();
        boolean success = configuration.enrich(httpExchange).isSuccess();
        assertThat(success).isFalse();
    }



    private HttpExchange getDummyHttpExchange() {
        Map<String, List<String>> requestHeaders = Maps.newHashMap();
        requestHeaders.put("X-dummy", Lists.newArrayList("blabla"));
        HttpRequest httpRequest = new HttpRequest("http://www.titi.com", DUMMY_METHOD, DUMMY_BODY_TYPE);
        httpRequest.setHeaders(requestHeaders);
        httpRequest.setBodyAsString("stuff");
        HttpResponse httpResponse = new HttpResponse(200, "OK");
        httpResponse.setResponseBody("my response");
        Map<String, List<String>> responseHeaders = Maps.newHashMap();
        responseHeaders.put("Content-Type", Lists.newArrayList("application/json"));
        httpResponse.setResponseHeaders(responseHeaders);
        return new HttpExchange(
                httpRequest,
                httpResponse,
                245L,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
    }
}