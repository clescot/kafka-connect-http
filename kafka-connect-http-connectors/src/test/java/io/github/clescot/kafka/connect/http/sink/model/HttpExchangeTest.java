package io.github.clescot.kafka.connect.http.sink.model;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.SchemaLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.client.ahc.AHCHttpClient.SUCCESS;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


public class HttpExchangeTest {

    public static final String DUMMY_TOPIC = "dummy_topic";
    MockSchemaRegistryClient schemaRegistryClient;
    private KafkaJsonSchemaSerializer serializer;

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

        serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

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
    void test_serialize_http_exchange() throws RestClientException, IOException {
        int statusCode = 200;
        HttpExchange httpExchange = new HttpExchange(
                getDummyHttpRequest(),
                getDummyHttpResponse(statusCode),
                745L,
                OffsetDateTime.now(),
                new AtomicInteger(2),
                SUCCESS
        );


        byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpExchange);
        assertThat(bytes).isNotEmpty();

    }
}

