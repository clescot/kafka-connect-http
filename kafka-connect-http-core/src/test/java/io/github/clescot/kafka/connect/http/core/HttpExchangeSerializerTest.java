package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class HttpExchangeSerializerTest {
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_METHOD = "POST";
    private static final String DUMMY_BODY_TYPE = "STRING";

    @Test
    void serialize(){
        HttpRequest httpRequest = new HttpRequest();
        HttpResponse httpResponse= new HttpResponse();
        HttpExchange httpExchange = new HttpExchange(httpRequest,httpResponse,100, OffsetDateTime.now(),new AtomicInteger(1),true);
        HttpExchangeSerializer httpExchangeSerializer = new HttpExchangeSerializer();
        byte[] serialized = httpExchangeSerializer.serialize("dummy", httpExchange);
        assertThat(serialized).isNotNull();
    }


    @Test
    void test_http_exchange_json_serialization() throws JsonProcessingException, JSONException {
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String httpExchangeAsString = objectMapper.writeValueAsString(dummyHttpExchange);
        String expectedJSON = "" +
                "{\n" +
                "  \"durationInMillis\": 245,\n" +
                "  \"moment\": 1668388166.569457181,\n" +
                "  \"attempts\": 1,\n" +
                "  \"success\": true,\n" +
                "  \"httpResponse\": {\n" +
                "    \"statusCode\": 200,\n" +
                "    \"statusMessage\": \"OK\",\n" +
                "    \"responseBody\": \"my response\",\n" +
                "    \"responseHeaders\": {\n" +
                "      \"Content-Type\": [\"application/json\"]\n" +
                "    }\n" +
                "  },\n" +
                "  \"httpRequest\": {\n" +
                "    \"url\": \"http://www.titi.com\",\n" +
                "    \"headers\": {\n" +
                "      \"X-dummy\": [\n" +
                "        \"blabla\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"method\": \"" + DUMMY_METHOD + "\",\n" +
                "    \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "    \"bodyAsForm\": {},\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": [],\n" +
                "    \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "  }\n" +
                "}";

        JSONAssert.assertEquals(expectedJSON, httpExchangeAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true)
                ));


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