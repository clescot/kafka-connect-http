package io.github.clescot.kafka.connect.http.core;

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

import static io.github.clescot.kafka.connect.http.core.HttpRequest.BODY_AS_BYTE_ARRAY;
import static io.github.clescot.kafka.connect.http.core.SchemaLoader.*;
import static org.assertj.core.api.Assertions.assertThat;

class HttpRequestTest {


    private static final String DUMMY_BODY_AS_STRING = "stuff";
    private static final String DUMMY_TOPIC = "myTopic";

    private SchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    void setup() throws RestClientException, IOException {
        schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));
        //Register http part
        ParsedSchema parsedPartSchema = loadHttpPartSchema();
        schemaRegistryClient.register("httpPart", parsedPartSchema);
        //register http request
        ParsedSchema parsedHttpRequestSchema = loadHttpRequestSchema();
        schemaRegistryClient.register("httpRequest", parsedHttpRequestSchema);
        //register http response
        ParsedSchema parsedHttpResponseSchema = loadHttpResponseSchema();
        schemaRegistryClient.register("httpResponse", parsedHttpResponseSchema);
        //register http exchange
        ParsedSchema parsedHttpExchangeSchema = loadHttpExchangeSchema();
        schemaRegistryClient.register("httpExchange", parsedHttpExchangeSchema);
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

            String expectedHttpRequest = "{\n" +
                    "  \"url\": \"http://www.stuff.com\",\n" +
                    "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                    "  \"method\": \"GET\",\n" +
                    "  \"bodyAsString\": \"stuff\",\n" +
                    "  \"bodyType\": \"STRING\"\n" +
                    "}";

            String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
            JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest, true);
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

            String expectedHttpRequest = "{\n" +
                    "  \"url\" : \"http://www.stuff.com\",\n" +
                    "  \"headers\" : {\n" +
                    "    \"X-request-id\" : [ \"aaaa-4466666-111\" ],\n" +
                    "    \"X-correlation-id\" : [ \"sfds-55-77\" ],\n" +
                    "    \"Content-Type\" : [ \"application/octet-stream\" ]\n" +
                    "  },\n" +
                    "  \"method\" : \"POST\",\n" +
                    "  \"bodyAsByteArray\":\"c3R1ZmY=\"," +
                    "  \"bodyType\" : \"BYTE_ARRAY\"\n" +
                    "}";

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
                    HttpRequest.Method.POST, headers, HttpRequest.BodyType.MULTIPART
            );
            List<HttpPart> httpParts = Lists.newArrayList();
            HttpPart part1 = new HttpPart("part1".getBytes(StandardCharsets.UTF_8));
            httpParts.add(part1);
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            httpParts.add(part2);
            HttpPart part3 = new HttpPart("part3".getBytes(StandardCharsets.UTF_8));
            httpParts.add(part3);
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
                    HttpRequest.Method.POST, headers, HttpRequest.BodyType.MULTIPART
            );
            List<HttpPart> httpParts = Lists.newArrayList();
            URL resourceURL = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            HttpPart part1;
            if (resourceURL != null) {
                File file = new File(resourceURL.toURI());
                part1 = new HttpPart("parameter1", "parameterValue1", file);
            } else {
                throw new IllegalStateException("file not found");
            }

            httpParts.add(part1);
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            httpParts.add(part2);
            HttpPart part3 = new HttpPart("part3");
            httpParts.add(part3);
            httpRequest.setParts(httpParts);
            headers.put("X-correlation-id", Lists.newArrayList("sfds-55-77"));
            headers.put("X-request-id", Lists.newArrayList("aaaa-4466666-111"));
            httpRequest.setHeaders(headers);


            String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
            HttpRequest deserializedRequest = objectMapper.readValue(serializedHttpRequest, HttpRequest.class);
            assertThat(httpRequest).isEqualTo(deserializedRequest);
            List<HttpPart> deserializedRequestParts = deserializedRequest.getParts();
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
            String httpRequestAsString = "{\n" +
                    "  \"url\": \"http://www.stuff.com\",\n" +
                    "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                    "  \"method\": \"GET\",\n" +
                    "\"bodyType\":\"STRING\", " +
                    "\"bodyAsString\":\"stuff\" " +
                    "}";

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

            String httpRequestAsString = "{\n" +
                    "  \"url\": \"http://www.stuff.com\",\n" +
                    "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                    "  \"method\": \"POST\",\n" +
                    "  \"bodyAsByteArray\": \"c3R1ZmY=\",\n" +
                    "  \"bodyType\": \"BYTE_ARRAY\"\n" +
                    "}";

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


            KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);


            byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);
            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));

            //like in kafka connect Sink connector, convert byte[] to struct
            Map<String, String> jsonSchemaDeserializerConfig = Maps.newHashMap();
            jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeserializerConfig, HttpRequest.class);
            HttpRequest deserializedHttpRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);
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


            KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);


            byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);
            System.out.println("bytesArray:" + Arrays.toString(bytes));

            //like in kafka connect Sink connector, convert byte[] to struct
            Map<String, String> jsonSchemaDeserializerConfig = Maps.newHashMap();
            jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeserializerConfig, HttpRequest.class);
            HttpRequest deserializedHttpRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);
            assertThat(deserializedHttpRequest).isEqualTo(httpRequest);
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
            byte[] fromConnectData = jsonSchemaConverter.fromConnectData(DUMMY_TOPIC, HttpRequest.SCHEMA, httpRequest.toStruct());
            //like in kafka connect Sink connector, convert byte[] to struct
            SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(DUMMY_TOPIC, fromConnectData);
            //then
            Schema schema = schemaAndValue.schema();
            assertThat(schema).isEqualTo(HttpRequest.SCHEMA);
            assertThat(schemaAndValue.value()).isEqualTo(httpRequest.toStruct());
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
            KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

            //when
            //serialize http as byte[]
            byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);

            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));

            //build serializer
            Map<String, String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeSerializerConfig, HttpRequest.class);

            HttpRequest deserializedRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);

            //then
            assertThat(deserializedRequest).isEqualTo(httpRequest);

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
            byte[] fromConnectData = jsonSchemaConverter.fromConnectData(DUMMY_TOPIC, HttpRequest.SCHEMA, httpRequest.toStruct());
            //like in kafka connect Sink connector, convert byte[] to struct
            SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(DUMMY_TOPIC, fromConnectData);
            //then
            Schema schema = schemaAndValue.schema();
            assertThat(schema).isEqualTo(HttpRequest.SCHEMA);
            assertThat(schemaAndValue.value()).isEqualTo(httpRequest.toStruct());
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
            KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

            //when
            //serialize http as byte[]
            byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);

            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));


            //build serializer
            Map<String, String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeSerializerConfig, HttpRequest.class);

            HttpRequest deserializedRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);

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
            headers.put("Content-Type", Lists.newArrayList("application/x-www-form-urlencoded"));
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
            KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);

            //when
            //serialize http as byte[]
            byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);

            System.out.println("bytesAsString:" + new String(bytes, StandardCharsets.UTF_8));


            //build serializer
            Map<String, String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpRequest.class.getName());
            jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + failUnknownProperties);
            KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient, jsonSchemaDeSerializerConfig, HttpRequest.class);

            HttpRequest deserializedRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);

            //then
            assertThat(deserializedRequest).isEqualTo(httpRequest);

        }

    }

    @Nested
    class TestConstructor {
        @Test
        void test_with_empty_struct() {
            //given
            Struct struct = new Struct(HttpRequest.SCHEMA);
            //when
            Assertions.assertThrows(NullPointerException.class, () -> new HttpRequest(struct));
        }

        @Test
        void test_with_struct_only_url() {
            //given
            Struct struct = new Struct(HttpRequest.SCHEMA);
            struct.put("url", "http://stuff.com");
            //when
            Assertions.assertThrows(NullPointerException.class, () -> new HttpRequest(struct));
        }

        @Test
        void test_with_struct_only_url_and_method() {
            //given
            Struct struct = new Struct(HttpRequest.SCHEMA);
            struct.put("url", "http://stuff.com");
            struct.put("method", "GET");
            //when
            Assertions.assertDoesNotThrow(() -> new HttpRequest(struct));
        }

        @Test
        void test_with_struct_nominal_case() {
            //given
            Struct struct = new Struct(HttpRequest.SCHEMA);
            String dummyUrl = "http://stuff.com";
            struct.put("url", dummyUrl);
            HttpRequest.Method dummyMethod = HttpRequest.Method.GET;
            struct.put("method", dummyMethod.name());
            String dummyBodyType = "STRING";
            struct.put("bodyType", dummyBodyType);
            struct.put("bodyAsString", DUMMY_BODY_AS_STRING);
            //when
            HttpRequest httpRequest = new HttpRequest(struct);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
            assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
            assertThat(httpRequest.getBodyAsString()).isEqualTo(DUMMY_BODY_AS_STRING);
        }

        @Test
        void test_with_struct_and_byte_array_nominal_case() {
            //given
            Struct httpRequestStruct = new Struct(HttpRequest.SCHEMA);
            String dummyUrl = "http://stuff.com";
            httpRequestStruct.put("url", dummyUrl);
            HttpRequest.Method dummyMethod = HttpRequest.Method.POST;
            httpRequestStruct.put("method", dummyMethod.name());

            String dummyBodyType = "BYTE_ARRAY";
            httpRequestStruct.put(HttpRequest.BODY_TYPE, dummyBodyType);
            httpRequestStruct.put(BODY_AS_BYTE_ARRAY, Base64.getEncoder().encodeToString(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8)));

            //when
            HttpRequest httpRequest = new HttpRequest(httpRequestStruct);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
            assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
            assertThat(httpRequest.getBodyAsByteArray()).isEqualTo(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
        }

        @Test
        void test_with_struct_and_parts_nominal_case() {
            //given
            Struct httpRequestStruct = new Struct(HttpRequest.SCHEMA);
            String dummyUrl = "http://stuff.com";
            httpRequestStruct.put("url", dummyUrl);
            HttpRequest.Method dummyMethod = HttpRequest.Method.POST;
            httpRequestStruct.put("method", dummyMethod.name());

            String dummyBodyType = "MULTIPART";
            httpRequestStruct.put(HttpRequest.BODY_TYPE, dummyBodyType);

            List<Struct> parts = Lists.newArrayList();
            HttpPart part1 = new HttpPart("part1".getBytes(StandardCharsets.UTF_8));
            parts.add(part1.toStruct());
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            parts.add(part2.toStruct());
            HttpPart part3 = new HttpPart("part3".getBytes(StandardCharsets.UTF_8));
            parts.add(part3.toStruct());

            httpRequestStruct.put(HttpRequest.PARTS, parts);

            //when
            HttpRequest httpRequest = new HttpRequest(httpRequestStruct);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
            assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
            List<HttpPart> httpParts = httpRequest.getParts();
            assertThat(httpParts).hasSameSizeAs(parts);
            assertThat(httpParts).contains(part1, part2, part3);
        }
    }

    @Nested
    class TestClone{
        @Test
        void test_clone_with_body_as_string() {
            //given
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
            //when
            HttpRequest clonedHttpRequest = httpRequest.clone();
            //then
            assertThat(clonedHttpRequest).isEqualTo(httpRequest);
        }

        @Test
        void test_clone_with_body_as_byte_array() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            headers.put("Content-Type",Lists.newArrayList("application/octet-stream"));
            httpRequest.setHeaders(headers);
            //when
            HttpRequest clonedHttpRequest = httpRequest.clone();
            //then
            assertThat(clonedHttpRequest).isEqualTo(httpRequest);
        }
    }

    @Nested
    class TestEqualsAndHashCode {

        @Test
        void test_equals_and_hashcode_headers_and_body_as_string() {
            //given
            HttpRequest httpRequest1 = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest1.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest1.setHeaders(headers);

            HttpRequest httpRequest2 = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest2.setBodyAsString(DUMMY_BODY_AS_STRING);
            httpRequest2.setHeaders(headers);

            //when
            assertThat(httpRequest1).isEqualTo(httpRequest2);
            assertThat(httpRequest1.hashCode()).isEqualTo(httpRequest2.hashCode());

        }

        @Test
        void test_equals_and_hashcode_headers_and_body_as_byte_array() {
            //given
            HttpRequest httpRequest1 = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest1.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest1.setHeaders(headers);

            HttpRequest httpRequest2 = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest2.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            httpRequest2.setHeaders(headers);

            //when
            assertThat(httpRequest1).isEqualTo(httpRequest2);
            assertThat(httpRequest1.hashCode()).isEqualTo(httpRequest2.hashCode());

        }
    }

    @Nested
    class TestGetContentType{

        @Test
        void test_get_content_type_with_string_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            headers.put("Content-Type", Lists.newArrayList("text/plain; charset=UTF-8"));
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            //when
            String contentType = httpRequest.getContentType();
            //then
            assertThat(contentType).isEqualTo("text/plain; charset=UTF-8");
        }

        @Test
        void test_get_content_type_with_byte_array_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );

            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            //when
            String contentType = httpRequest.getContentType();
            //then
            assertThat(contentType).isEqualTo("application/octet-stream");
        }

        @Test
        void test_get_content_type_without_headers_and_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );

            assertThat(httpRequest.getContentType()).isNull();

        }

        @Test
        void test_get_content_type_with_headers_and_without_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);

            assertThat(httpRequest.getContentType()).isNull();

        }

        @Test
        void test_get_content_type_with_headers_and_empty_content_type_and_without_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            headers.put("Content-Type", Lists.newArrayList(""));
            httpRequest.setHeaders(headers);

            assertThat(httpRequest.getContentType()).isEmpty();

        }
    }


    @Nested
    class TestGetBoundary{
        @Test
        void test_get_boundary_with_no_headers() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            //when
            String boundary = httpRequest.getBoundary();
            //then
            assertThat(boundary).isNull();
        }

        @Test
        void test_get_boundary_with_headers_but_no_boundary() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            httpRequest.setHeaders(headers);
            //when
            String boundary = httpRequest.getBoundary();
            //then
            assertThat(boundary).isNull();
        }

        @Test
        void test_get_boundary_with_headers_and_boundary() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList("multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW"));
            httpRequest.setHeaders(headers);
            //when
            String boundary = httpRequest.getBoundary();
            //then
            assertThat(boundary).isEqualTo("----WebKitFormBoundary7MA4YWxkTrZu0gW");
        }

    }
}