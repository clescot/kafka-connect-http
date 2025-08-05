package io.github.clescot.kafka.connect.http.core;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


public class HttpExchangeTest {
    public static final boolean SUCCESS = true;
    public static final String DUMMY_TOPIC = "dummy_topic";
    MockSchemaRegistryClient schemaRegistryClient;
    private KafkaJsonSchemaSerializer<HttpExchange> serializer;
    private KafkaJsonSchemaDeserializer<HttpExchange> deserializer;

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

    @BeforeEach
    void setup() throws RestClientException, IOException {
        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        boolean useOneOfForNullables = false;
        boolean failUnknownProperties = true;
        Map<String, String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, "true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "" + useOneOfForNullables);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);

        JsonSchemaProvider jsonSchemaProvider = new JsonSchemaProvider();

        schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(jsonSchemaProvider));
        ParsedSchema parsedSchemaRequest = SchemaLoader.loadHttpRequestSchema();
        schemaRegistryClient.register("httpRequest", parsedSchemaRequest);
        ParsedSchema parsedSchemaResponse = SchemaLoader.loadHttpResponseSchema();
        schemaRegistryClient.register("httpResponse", parsedSchemaResponse);
        ParsedSchema parsedSchemaExchange = SchemaLoader.loadHttpExchangeSchema();
        schemaRegistryClient.register("httpExchange", parsedSchemaExchange);

        serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

        Map<String,String> jsonSchemaDeserializerConfig = Maps.newHashMap();
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpExchange.class.getName());
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA,"true");
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES,""+true);
        deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient,jsonSchemaDeserializerConfig, HttpExchange.class);

    }

    @Nested
    class TestEqualsAndHashcode {
        @Test
        void test_null() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            assertThat(httpExchange).isNotNull();
        }

        @Test
        void test_different_class() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            assertThat(httpExchange).isNotEqualTo(new Object());
        }

        @Test
        void test_different_http_request() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange httpExchange1 = new HttpExchange(
                    new HttpRequest("http://www.example.com", HttpRequest.Method.GET),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            assertThat(httpExchange1).isNotEqualTo(httpExchange);
        }

        @Test
        void test_different_http_response() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange httpExchange1 = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(404),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            assertThat(httpExchange1).isNotEqualTo(httpExchange);
        }

        @Test
        void test_different_duration_in_millis() {
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
                    200,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            assertThat(httpExchange1).isNotEqualTo(httpExchange);
        }

        @Test
        void test_nominal_case() {
            OffsetDateTime now = OffsetDateTime.now();
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    now,
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange httpExchange1 = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    now,
                    new AtomicInteger(2),
                    SUCCESS);
            assertThat(httpExchange1).isEqualTo(httpExchange);
        }

        @Test
        void test_nominal_case_detail() {
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
            assertThat(httpExchange.getHttpResponse().getBodyAsString()).isEqualTo(responseBody);
            assertThat(httpExchange.getHttpResponse().getStatusCode()).isEqualTo(statusCode);
        }

        @Test
        void generate_json_schema() throws IOException {
            int statusCode = 200;
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(statusCode),
                    745L,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS
            );

            //get JSON schema
            SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
            boolean useOneOfForNullables = false;
            boolean failUnknownProperties = true;
            JsonSchema expectedJsonSchema = JsonSchemaUtils.getSchema(
                    httpExchange,
                    jsonSchemaSpecification,
                    useOneOfForNullables,
                    failUnknownProperties,
                    schemaRegistryClient
            );
            assertThat(expectedJsonSchema).isNotNull();
        }

        @Test
        void test_serialize_http_exchange() {
            int statusCode = 200;
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(statusCode),
                    745L,
                    OffsetDateTime.now(ZoneId.of("UTC")),
                    new AtomicInteger(2),
                    SUCCESS
            );


            byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpExchange);
            assertThat(bytes).isNotEmpty();
            HttpExchange deserializedHttpExchange = deserializer.deserialize(DUMMY_TOPIC, bytes);
            assertThat(deserializedHttpExchange.getHttpRequest()).isEqualTo(httpExchange.getHttpRequest());
            assertThat(deserializedHttpExchange.getHttpResponse()).isEqualTo(httpExchange.getHttpResponse());
            assertThat(deserializedHttpExchange).isEqualTo(httpExchange);
        }
    }

    @Nested
    class TestClone{
        @Test
        void test_clone() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange clone = (HttpExchange) httpExchange.clone();
            assertThat(clone).isEqualTo(httpExchange);
            assertThat(clone.getHttpRequest()).isEqualTo(httpExchange.getHttpRequest());
            assertThat(clone.getHttpResponse()).isEqualTo(httpExchange.getHttpResponse());
        }
    }

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
            assertThat(httpExchange.toStruct()).isNotNull();
            assertThat(httpExchange.toStruct().get("httpRequest")).isNotNull();
            assertThat(httpExchange.toStruct().get("httpResponse")).isNotNull();
            assertThat(httpExchange.toStruct().get("durationInMillis")).isEqualTo(100L);
            assertThat(httpExchange.toStruct().get("moment")).isEqualTo(moment.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
            assertThat(httpExchange.toStruct().get("attempts")).isEqualTo(attempts.get());
        }
    }
}


