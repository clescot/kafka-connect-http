package io.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.json.JsonSchemaConverterConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.github.clescot.core.http.BodyType;
import io.github.clescot.core.http.HttpPart;
import io.github.clescot.core.http.HttpRequest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static io.github.clescot.core.http.HttpRequest.BODY_AS_BYTE_ARRAY_FIELD;
import static io.github.clescot.core.http.MediaType.APPLICATION_X_WWW_FORM_URLENCODED;
import static io.github.clescot.core.http.SchemaLoader.*;
import static io.github.clescot.core.http.SchemaLoader.loadHttpExchangeSchema;
import static org.assertj.core.api.Assertions.assertThat;

class HttpRequestAdapterTest {
    private static final String DUMMY_BODY_AS_STRING = "stuff";
    private static final String DUMMY_TOPIC = "myTopic";

    private SchemaRegistryClient schemaRegistryClient;
    private KafkaJsonSchemaSerializer<HttpRequest> serializer;
    private KafkaJsonSchemaDeserializer<HttpRequest> deserializer;
    private static final String RESPONSE_TOPIC = "dummy_response";
    private static final String REQUEST_TOPIC = "dummy_request";
    private static final String EXCHANGE_TOPIC = "dummy_exchange";


    @BeforeEach
    void setup() throws RestClientException, IOException {
        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,""+false);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA,""+true);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,""+true);

        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));
        //Register http part
        ParsedSchema parsedPartSchema = loadHttpPartSchema();
        schemaRegistryClient.register("httpPart"+"-value", parsedPartSchema);
        //register http request
        ParsedSchema parsedHttpRequestSchema = loadHttpRequestSchema();
        schemaRegistryClient.register(REQUEST_TOPIC+"-value", parsedHttpRequestSchema);
        //register http response
        ParsedSchema parsedHttpResponseSchema = loadHttpResponseSchema();
        schemaRegistryClient.register(RESPONSE_TOPIC+"-value", parsedHttpResponseSchema);
        //register http exchange
        ParsedSchema parsedHttpExchangeSchema = loadHttpExchangeSchema();
        schemaRegistryClient.register(EXCHANGE_TOPIC+"-value", parsedHttpExchangeSchema);

        serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);
        Map<String,String> jsonSchemaDeserializerConfig = Maps.newHashMap();
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpRequest.class.getName());
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA,"true");
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES,""+true);
        deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient,jsonSchemaDeserializerConfig, HttpRequest.class);
    }


    @Nested
    class TestConstructor {
        @Test
        void test_with_empty_struct() {
            //given
            Struct struct = new Struct(HttpRequestAdapter.SCHEMA);
            //when
            Assertions.assertThrows(NullPointerException.class, () -> HttpRequestAdapter.from(struct).toHttpRequest());
        }

        @Test
        void test_with_struct_only_url() {
            //given
            Struct struct = new Struct(HttpRequestAdapter.SCHEMA);
            struct.put("url", "http://stuff.com");
            //when
            Assertions.assertThrows(NullPointerException.class, () -> HttpRequestAdapter.from(struct).toHttpRequest());
        }

        @Test
        void test_with_struct_only_url_and_method() {
            //given
            Struct struct = new Struct(HttpRequestAdapter.SCHEMA);
            struct.put("url", "http://stuff.com");
            struct.put("method", "GET");
            //when
            Assertions.assertDoesNotThrow(() -> HttpRequestAdapter.from(struct));
        }

        @Test
        void test_with_struct_nominal_case() {
            //given
            Struct struct = new Struct(HttpRequestAdapter.SCHEMA);
            String dummyUrl = "http://stuff.com";
            struct.put("url", dummyUrl);
            HttpRequest.Method dummyMethod = HttpRequest.Method.GET;
            struct.put("method", dummyMethod.name());
            String dummyBodyType = "STRING";
            struct.put("bodyType", dummyBodyType);
            struct.put("bodyAsString", DUMMY_BODY_AS_STRING);
            //when
            HttpRequest httpRequest = HttpRequestAdapter.from(struct).toHttpRequest();
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
            assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
            assertThat(httpRequest.getBodyAsString()).isEqualTo(DUMMY_BODY_AS_STRING);
        }

        @Test
        void test_with_struct_and_byte_array_nominal_case() {
            //given
            Struct httpRequestStruct = new Struct(HttpRequestAdapter.SCHEMA);
            String dummyUrl = "http://stuff.com";
            httpRequestStruct.put("url", dummyUrl);
            HttpRequest.Method dummyMethod = HttpRequest.Method.POST;
            httpRequestStruct.put("method", dummyMethod.name());

            String dummyBodyType = "BYTE_ARRAY";
            httpRequestStruct.put(HttpRequest.BODY_TYPE_FIELD, dummyBodyType);
            httpRequestStruct.put(BODY_AS_BYTE_ARRAY_FIELD, new String(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8)));

            //when
            HttpRequest httpRequest = HttpRequestAdapter.from(httpRequestStruct).toHttpRequest();
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
            assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
            assertThat(httpRequest.getBodyAsByteArray()).isEqualTo(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
        }

        @Test
        void test_with_struct_and_parts_nominal_case() {
            //given
            Struct httpRequestStruct = new Struct(HttpRequestAdapter.SCHEMA);
            String dummyUrl = "http://stuff.com";
            httpRequestStruct.put("url", dummyUrl);
            HttpRequest.Method dummyMethod = HttpRequest.Method.POST;
            httpRequestStruct.put("method", dummyMethod.name());

            String dummyBodyType = "MULTIPART";
            httpRequestStruct.put(HttpRequest.BODY_TYPE_FIELD, dummyBodyType);

            Map<String,Struct> parts = Maps.newHashMap();
            HttpPart part1 = new HttpPart("part1".getBytes(StandardCharsets.UTF_8));
            parts.put("part1",part1.toStruct());
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            parts.put("part2",part2.toStruct());
            HttpPart part3 = new HttpPart("part3".getBytes(StandardCharsets.UTF_8));
            parts.put("part3",part3.toStruct());

            httpRequestStruct.put(HttpRequest.PARTS_FIELD, parts);

            //when
            HttpRequest httpRequest = HttpRequestAdapter.from(httpRequestStruct).toHttpRequest();
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
            assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
            Map<String,HttpPart> httpParts = httpRequest.getParts();
            assertThat(httpParts)
                    .hasSameSizeAs(parts)
                    .contains(
                            Map.entry("part1",part1),
                            Map.entry("part2", part2),
                            Map.entry("part3",part3)
                    );
        }
    }
    @Nested
    class TestSerialization {
        @Test
        void test_serialization() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            httpRequest.setHeaders(headers);

            String expectedHttpRequest = """
                    {
                      "url": "http://www.stuff.com",
                      "headers":{"X-request-id":["aaaa-4466666-111"],"X-correlation-id":["sfds-55-77"]},
                      "method": "GET",
                      "bodyAsString": "stuff",
                      "bodyType": "STRING"
                    }
                    """;

            String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
            JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest, true);
            byte[] serializedRequest = serializer.serialize(REQUEST_TOPIC, httpRequest);
            assertThat(serializedRequest).isNotEmpty();
            HttpRequest deserializedRequest = deserializer.deserialize(RESPONSE_TOPIC, serializedRequest);
            assertThat(deserializedRequest).isEqualTo(httpRequest);
        }

        @Test
        void test_serialization_with_attributes() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, Object> attributes = Maps.newHashMap();
            attributes.put("attr1", "value1");
            attributes.put("attr2", "value2");
            httpRequest.setAttributes(attributes);
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            httpRequest.setHeaders(headers);

            String expectedHttpRequest = """
                    {
                      "url": "http://www.stuff.com",
                      "headers": {
                        "X-request-id": [
                          "aaaa-4466666-111"
                        ],
                        "X-correlation-id": [
                          "sfds-55-77"
                        ]
                      },
                      "method": "GET",
                      "attributes": {
                        "attr2": "value2",
                        "attr1": "value1"
                      },
                      "bodyAsString": "stuff",
                      "bodyType": "STRING"
                    }
                    """;

            String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
            JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest, true);
            byte[] serializedRequest = serializer.serialize(REQUEST_TOPIC, httpRequest);
            assertThat(serializedRequest).isNotEmpty();
            HttpRequest deserializedRequest = deserializer.deserialize(RESPONSE_TOPIC, serializedRequest);
            assertThat(deserializedRequest).isEqualTo(httpRequest);
        }

        @Test
        void test_serialization_with_byte_array() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            headers.put("Content-Type", Lists.newArrayList("application/octet-stream"));
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));

            String expectedHttpRequest = """
                    {
                      "url" : "http://www.stuff.com",
                      "headers" : {
                        "X-request-id" : [ "aaaa-4466666-111" ],
                        "X-correlation-id" : [ "sfds-55-77" ],
                        "Content-Type" : [ "application/octet-stream" ]
                      },
                      "method" : "POST",
                      "bodyAsByteArray":"c3R1ZmY=",
                      "bodyType" : "BYTE_ARRAY"
                    }
                    """;

            String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
            JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest, true);
        }

        @Test
        void test_serialization_with_multipart() throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList(
                    "multipart/form-data; boundary=45789ee5"));
            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST, headers, BodyType.MULTIPART
            );
            Map<String, HttpPart> httpParts = Maps.newHashMap();
            HttpPart part1 = new HttpPart("part1".getBytes(StandardCharsets.UTF_8));
            httpParts.put("id1", part1);
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            httpParts.put("id2", part2);
            HttpPart part3 = new HttpPart("part3".getBytes(StandardCharsets.UTF_8));
            httpParts.put("id3", part3);
            httpRequest.setParts(httpParts);
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            httpRequest.setHeaders(headers);


            String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
            HttpRequest deserializedRequest = objectMapper.readValue(serializedHttpRequest, HttpRequest.class);
            assertThat(httpRequest).isEqualTo(deserializedRequest);
        }

        @Test
        void test_serialization_with_multipart_and_file() throws JsonProcessingException, URISyntaxException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            objectMapper.registerModule(new Jdk8Module());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList(
                    "multipart/form-data; boundary=45789ee5"));
            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST, headers, BodyType.MULTIPART
            );
            Map<String, HttpPart> httpParts = Maps.newHashMap();
            URL resourceURL = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            HttpPart part1;
            if (resourceURL != null) {
                File file = new File(resourceURL.toURI());
                part1 = new HttpPart("fileName", file);
            } else {
                throw new IllegalStateException("file not found");
            }

            httpParts.put("part1", part1);
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            httpParts.put("part2", part2);
            HttpPart part3 = new HttpPart("part3");
            httpParts.put("part3", part3);
            httpRequest.setParts(httpParts);
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            httpRequest.setHeaders(headers);


            String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
            HttpRequest deserializedRequest = objectMapper.readValue(serializedHttpRequest, HttpRequest.class);
            assertThat(httpRequest).isEqualTo(deserializedRequest);
            Map<String, HttpPart> deserializedRequestParts = deserializedRequest.getParts();
            assertThat(deserializedRequestParts).hasSameSizeAs(httpParts);
        }

        @Test
        void test_deserialization() throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            HttpRequest expectedHttpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            expectedHttpRequest.setHeaders(headers);
            expectedHttpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            String httpRequestAsString = """
                    {
                      "url": "http://www.stuff.com",
                      "headers":{"X-request-id":["aaaa-4466666-111"],"X-correlation-id":["sfds-55-77"]},
                      "method": "GET",
                    "bodyType":"STRING",\s
                    "bodyAsString":"stuff"\s
                    }
                    """;

            HttpRequest parsedHttpRequest = objectMapper.readValue(httpRequestAsString, HttpRequest.class);
            assertThat(parsedHttpRequest).isEqualTo(expectedHttpRequest);
        }

        @Test
        void test_deserialization_with_byte_array() throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            HttpRequest expectedHttpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            expectedHttpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            headers.put("Content-Type", Lists.newArrayList("application/octet-stream"));
            expectedHttpRequest.setHeaders(headers);

            String httpRequestAsString = """
                    {
                      "url": "http://www.stuff.com",
                      "headers":{"X-request-id":["aaaa-4466666-111"],"X-correlation-id":["sfds-55-77"]},
                      "method": "POST",
                      "bodyAsByteArray": "c3R1ZmY=",
                      "bodyType": "BYTE_ARRAY"
                    }
                    """;

            HttpRequest parsedHttpRequest = objectMapper.readValue(httpRequestAsString, HttpRequest.class);
            assertThat(parsedHttpRequest).isEqualTo(expectedHttpRequest);
        }

        @Test
        void test_deserialization_with_byte_array_and_attributes() throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            HttpRequest expectedHttpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            expectedHttpRequest.setAttributes(Map.of("attr1", "value1", "attr2", "value2"));
            expectedHttpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            headers.put("Content-Type", Lists.newArrayList("application/octet-stream"));
            expectedHttpRequest.setHeaders(headers);

            String httpRequestAsString = """
                    {
                      "url": "http://www.stuff.com",
                      "attributes":{"attr1":"value1","attr2":"value2"},
                      "headers":{"X-request-id":["aaaa-4466666-111"],"X-correlation-id":["sfds-55-77"]},
                      "method": "POST",
                      "bodyAsByteArray": "c3R1ZmY=",
                      "bodyType": "BYTE_ARRAY"
                    }
                    """;

            HttpRequest parsedHttpRequest = objectMapper.readValue(httpRequestAsString, HttpRequest.class);
            assertThat(parsedHttpRequest).isEqualTo(expectedHttpRequest);
        }

        @Test
        void test_serialize_and_deserialize_http_request_with_low_level_serializer() {
            //given

            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);
            SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
            boolean useOneOfForNullables = false;
            boolean failUnknownProperties = true;

            //serialize http as byte[]
            Map<String, String> jsonSchemaSerializerConfig = Maps.newHashMap();
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecification.toString());
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, "true");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "" + useOneOfForNullables);
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);


            KafkaJsonSchemaSerializer<HttpRequest> mySerializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);


            byte[] bytes = mySerializer.serialize(DUMMY_TOPIC, httpRequest);
            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));

            //like in kafka connect Sink connector, convert byte[] to struct
            Map<String, String> jsonSchemaDeserializerConfig = Maps.newHashMap();
            jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            KafkaJsonSchemaDeserializer<HttpRequest> myDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeserializerConfig, HttpRequest.class);
            HttpRequest deserializedHttpRequest = myDeserializer.deserialize(DUMMY_TOPIC, bytes);
            assertThat(deserializedHttpRequest).isEqualTo(httpRequest);
        }

        @Test
        void test_serialize_and_deserialize_http_request_with_byte_array_and_low_level_serializer() {
            //given

            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            headers.put("Content-Type", Lists.newArrayList("application/octet-stream"));
            httpRequest.setHeaders(headers);
            SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
            boolean useOneOfForNullables = false;
            boolean failUnknownProperties = true;

            //serialize http as byte[]
            Map<String, String> jsonSchemaSerializerConfig = Maps.newHashMap();
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecification.toString());
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, "true");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "" + useOneOfForNullables);
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);


            KafkaJsonSchemaSerializer<HttpRequest> mySerializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);


            byte[] bytes = mySerializer.serialize(DUMMY_TOPIC, httpRequest);
            System.out.println("bytesArray:" + Arrays.toString(bytes));

            //like in kafka connect Sink connector, convert byte[] to struct
            Map<String, String> jsonSchemaDeserializerConfig = Maps.newHashMap();
            jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            KafkaJsonSchemaDeserializer<HttpRequest> myDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeserializerConfig, HttpRequest.class);
            HttpRequest deserializedHttpRequest = myDeserializer.deserialize(DUMMY_TOPIC, bytes);
            assertThat(deserializedHttpRequest).isEqualTo(httpRequest);
        }


        @Test
        void test_serialize_and_deserialize_http_request_with_body_as_string() {
            //given

            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);

            SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
            boolean useOneOfForNullables = false;
            boolean failUnknownProperties = false;

            //build serializer
            Map<String, String> jsonSchemaSerializerConfig = Maps.newHashMap();
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecification.toString());
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, "true");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "" + useOneOfForNullables);
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaSerializer<HttpRequest> mySerializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

            //when
            //serialize http as byte[]
            byte[] bytes = mySerializer.serialize(DUMMY_TOPIC, httpRequest);

            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));

            //build serializer
            Map<String, String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaDeserializer<HttpRequest> myDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeSerializerConfig, HttpRequest.class);

            HttpRequest deserializedRequest = myDeserializer.deserialize(DUMMY_TOPIC, bytes);

            //then
            assertThat(deserializedRequest).isEqualTo(httpRequest);

        }


        @Test
        void test_serialize_and_deserialize_http_request_with_body_as_byte_array() {
            //given

            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            headers.put("Content-Type", Lists.newArrayList("application/octet-stream"));
            httpRequest.setHeaders(headers);

            SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
            boolean useOneOfForNullables = false;
            boolean failUnknownProperties = false;


            //build serializer
            Map<String, String> jsonSchemaSerializerConfig = Maps.newHashMap();
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecification.toString());
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, "true");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "" + useOneOfForNullables);
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaSerializer<HttpRequest> mySerializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

            //when
            //serialize http as byte[]
            byte[] bytes = mySerializer.serialize(DUMMY_TOPIC, httpRequest);

            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));


            //build serializer
            Map<String, String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaDeserializer<HttpRequest> myDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeSerializerConfig, HttpRequest.class);

            HttpRequest deserializedRequest = myDeserializer.deserialize(DUMMY_TOPIC, bytes);

            //then
            assertThat(deserializedRequest).isEqualTo(httpRequest);

        }


        @Test
        void test_serialize_and_deserialize_http_request_with_body_as_form() {
            //given

            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            Map<String, String> form = Maps.newHashMap();
            form.put("key1", "value1");
            form.put("key2", "value2");
            httpRequest.setBodyAsForm(form);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            headers.put("Content-Type", Lists.newArrayList(APPLICATION_X_WWW_FORM_URLENCODED));
            httpRequest.setHeaders(headers);

            SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
            boolean useOneOfForNullables = false;
            boolean failUnknownProperties = false;


            //build serializer
            Map<String, String> jsonSchemaSerializerConfig = Maps.newHashMap();
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecification.toString());
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, "true");
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "" + useOneOfForNullables);
            jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaSerializer<HttpRequest> mySerializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

            //when
            //serialize http as byte[]
            byte[] bytes = mySerializer.serialize(DUMMY_TOPIC, httpRequest);

            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));


            //build serializer
            Map<String, String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaDeserializer<HttpRequest> myDeserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeSerializerConfig, HttpRequest.class);

            HttpRequest deserializedRequest = myDeserializer.deserialize(DUMMY_TOPIC, bytes);

            //then
            assertThat(deserializedRequest).isEqualTo(httpRequest);

        }


        @Test
        void test_serialize_and_deserialize_http_request_with_body_as_string_with_converter() {
            //given

            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);


            JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
            Map<String, String> converterConfig = Maps.newHashMap();
            converterConfig.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            converterConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaConverter.configure(converterConfig, false);

            //when
            byte[] fromConnectData = jsonSchemaConverter.fromConnectData(DUMMY_TOPIC, HttpRequestAdapter.SCHEMA, HttpRequestAdapter.from(httpRequest).toStruct());
            //like in kafka connect Sink connector, convert byte[] to struct
            SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(DUMMY_TOPIC, fromConnectData);
            //then
            Schema schema = schemaAndValue.schema();
            assertThat(schema).isEqualTo(HttpRequestAdapter.SCHEMA);
            assertThat(schemaAndValue.value()).isEqualTo(HttpRequestAdapter.from(httpRequest).toStruct());
        }


        @Test
        void test_serialize_and_deserialize_http_request_with_body_as_array_with_converter() {
            //given

            //build httpRequest
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);


            JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
            Map<String, String> converterConfig = Maps.newHashMap();
            converterConfig.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            converterConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaConverter.configure(converterConfig, false);

            //when
            byte[] fromConnectData = jsonSchemaConverter.fromConnectData(DUMMY_TOPIC, HttpRequestAdapter.SCHEMA, HttpRequestAdapter.from(httpRequest).toStruct());
            //like in kafka connect Sink connector, convert byte[] to struct
            SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(DUMMY_TOPIC, fromConnectData);
            //then
            Schema schema = schemaAndValue.schema();
            assertThat(schema).isEqualTo(HttpRequestAdapter.SCHEMA);
            assertThat(schemaAndValue.value()).isEqualTo(HttpRequestAdapter.from(httpRequest).toStruct());
        }
    }
}