package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.json.JsonSchemaConverterConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
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
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class HttpRequestTest {


    private static final String DUMMY_BODY_AS_STRING = "stuff";
    private static final String DUMMY_TOPIC = "myTopic";

    @Test
    void test_serialization() throws JsonProcessingException, JSONException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.GET,
                "STRING"
        );
        httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
        Map<String,List<String>> headers = Maps.newHashMap();
        headers.put("X-correlation-id",Lists.newArrayList("sfds-55-77"));
        headers.put("X-request-id",Lists.newArrayList("aaaa-4466666-111"));
        httpRequest.setHeaders(headers);

        String expectedHttpRequest = "{\n" +
                "  \"url\": \"http://www.stuff.com\",\n" +
                "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                "  \"method\": \"GET\",\n" +
                "  \"bodyAsString\": \"stuff\",\n" +
                "  \"bodyAsForm\": {},\n" +
                "  \"bodyAsByteArray\": \"\",\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"STRING\"\n" +
                "}";

        String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
        JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest,true);
    }

    @Test
    void test_serialization_with_byte_array() throws JsonProcessingException, JSONException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.POST,
                HttpRequest.BodyType.BYTE_ARRAY.name()
        );
        httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
        Map<String,List<String>> headers = Maps.newHashMap();
        headers.put("X-correlation-id",Lists.newArrayList("sfds-55-77"));
        headers.put("X-request-id",Lists.newArrayList("aaaa-4466666-111"));
        httpRequest.setHeaders(headers);

        String expectedHttpRequest = "{\n" +
                "  \"url\": \"http://www.stuff.com\",\n" +
                "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                "  \"method\": \"POST\",\n" +
                "  \"bodyAsString\": \"\",\n" +
                "  \"bodyAsForm\": {},\n" +
                "  \"bodyAsByteArray\": \"c3R1ZmY=\",\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"BYTE_ARRAY\"\n" +
                "}";

        String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
        JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest,true);
    }
    @Test
    void test_deserialization() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        HttpRequest expectedHttpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.GET,
                "STRING"
        );
        expectedHttpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
        Map<String,List<String>> headers = Maps.newHashMap();
        headers.put("X-correlation-id",Lists.newArrayList("sfds-55-77"));
        headers.put("X-request-id",Lists.newArrayList("aaaa-4466666-111"));
        expectedHttpRequest.setHeaders(headers);

        String httpRequestAsString = "{\n" +
                "  \"url\": \"http://www.stuff.com\",\n" +
                "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                "  \"method\": \"GET\",\n" +
                "  \"bodyAsString\": \"stuff\",\n" +
                "  \"bodyAsByteArray\": \"\",\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"STRING\"\n" +
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
                HttpRequest.Method.POST,
                HttpRequest.BodyType.BYTE_ARRAY.name()
        );
        expectedHttpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
        Map<String,List<String>> headers = Maps.newHashMap();
        headers.put("X-correlation-id",Lists.newArrayList("sfds-55-77"));
        headers.put("X-request-id",Lists.newArrayList("aaaa-4466666-111"));
        expectedHttpRequest.setHeaders(headers);

        String httpRequestAsString = "{\n" +
                "  \"url\": \"http://www.stuff.com\",\n" +
                "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                "  \"method\": \"POST\",\n" +
                "  \"bodyAsString\": \"\",\n" +
                "  \"bodyAsForm\": {},\n" +
                "  \"bodyAsByteArray\": \"c3R1ZmY=\",\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"BYTE_ARRAY\"\n" +
                "}";

        HttpRequest parsedHttpRequest = objectMapper.readValue(httpRequestAsString, HttpRequest.class);
        assertThat(parsedHttpRequest).isEqualTo(expectedHttpRequest);
    }

    @Test
    void test_serialize_and_deserialize_http_request_with_low_level_serializer() throws IOException {
        //given

        //build httpRequest
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.GET,
                "STRING"
        );
        httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);
        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        boolean useOneOfForNullables=false;
        boolean failUnknownProperties=true;
        //get JSON schema
        JsonSchema expectedJsonSchema = JsonSchemaUtils.getSchema(
                httpRequest,
                jsonSchemaSpecification,
                useOneOfForNullables,
                failUnknownProperties,
                null
                );

        //serialize http as byte[]
        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,""+useOneOfForNullables);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,""+failUnknownProperties);

        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));

        KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);


        byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);
        System.out.println("bytesAsString:"+new String(bytes, StandardCharsets.UTF_8));

        //like in kafka connect Sink connector, convert byte[] to struct
        Map<String,String> jsonSchemaDeserializerConfig = Maps.newHashMap();
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient,jsonSchemaDeserializerConfig,HttpRequest.class);
        HttpRequest deserializedHttpRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);
        assertThat(deserializedHttpRequest).isEqualTo(httpRequest);
    }
    @Test
    void test_serialize_and_deserialize_http_request_with_byte_array_and_low_level_serializer() throws IOException {
        //given

        //build httpRequest
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.POST,
                HttpRequest.BodyType.BYTE_ARRAY.name()
        );
        httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);
        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        boolean useOneOfForNullables=false;
        boolean failUnknownProperties=true;
        //get JSON schema
        JsonSchema expectedJsonSchema = JsonSchemaUtils.getSchema(
                httpRequest,
                jsonSchemaSpecification,
                useOneOfForNullables,
                failUnknownProperties,
                null
                );

        //serialize http as byte[]
        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,""+useOneOfForNullables);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,""+failUnknownProperties);

        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));

        KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);


        byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);
        System.out.println("bytesArray:"+ Arrays.toString(bytes));

        //like in kafka connect Sink connector, convert byte[] to struct
        Map<String,String> jsonSchemaDeserializerConfig = Maps.newHashMap();
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient,jsonSchemaDeserializerConfig,HttpRequest.class);
        HttpRequest deserializedHttpRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);
        assertThat(deserializedHttpRequest).isEqualTo(httpRequest);
    }

    @Test
    void test_serialize_http_request_with_serializer_and_deserialize_with_high_level_converter() throws IOException {
        //given

        //build httpRequest
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.GET,
                "STRING"
        );
        httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);

        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        boolean useOneOfForNullables=false;
        boolean failUnknownProperties=false;
        //get JSON schema
        JsonSchema expectedJsonSchema = JsonSchemaUtils.getSchema(
                httpRequest,
                jsonSchemaSpecification,
                useOneOfForNullables,
                failUnknownProperties,
                null
        );

        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,""+useOneOfForNullables);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,""+failUnknownProperties);
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));
        KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);

        //serialize http as byte[]
        byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);

        System.out.println("bytesAsString:"+new String(bytes, StandardCharsets.UTF_8));
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
        Map<String,String> converterConfig= Maps.newHashMap();
        converterConfig.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        converterConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpRequest.class.getName());
        jsonSchemaConverter.configure(converterConfig,false);

        //like in kafka connect Sink connector, convert byte[] to struct
        SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(DUMMY_TOPIC, bytes);
        Struct value = (Struct) schemaAndValue.value();
        assertThat(expectedJsonSchema.equals(value.schema()));
        //when
        HttpRequest parsedHttpRequest = HttpRequestAsStruct.Builder.anHttpRequest().withStruct(value).build();

        System.out.println(parsedHttpRequest);
    }


    @Test
    void test_serialize_and_deserialize_http_request_with_body_as_string_with_converter() {
        //given

        //build httpRequest
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.POST,
                HttpRequest.BodyType.STRING.name()
        );
        httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);

        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
        Map<String,String> converterConfig= Maps.newHashMap();
        converterConfig.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        converterConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpRequest.class.getName());
        jsonSchemaConverter.configure(converterConfig,false);

        //when
        HttpRequestAsStruct httpRequestAsStruct = new HttpRequestAsStruct(httpRequest);
        byte[] fromConnectData = jsonSchemaConverter.fromConnectData(DUMMY_TOPIC, HttpRequestAsStruct.SCHEMA, httpRequestAsStruct.toStruct());
        //like in kafka connect Sink connector, convert byte[] to struct
        SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(DUMMY_TOPIC, fromConnectData);
        //then
        Schema schema = schemaAndValue.schema();
        assertThat(schema).isEqualTo(HttpRequestAsStruct.SCHEMA);
        assertThat(schemaAndValue.value()).isEqualTo(httpRequestAsStruct.toStruct());
    }

    @Test
    void test_serialize_and_deserialize_http_request_with_body_as_string() {
        //given

        //build httpRequest
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.POST,
                HttpRequest.BodyType.STRING.name()
        );
        httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);

        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        boolean useOneOfForNullables=false;
        boolean failUnknownProperties=false;

        //build serializer
        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,""+useOneOfForNullables);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,""+failUnknownProperties);
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));
        KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);

        //when
        //serialize http as byte[]
        byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);

        System.out.println("bytesAsString:"+new String(bytes, StandardCharsets.UTF_8));

        //build serializer
        Map<String,String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
        jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpRequest.class.getClass().getName());
        jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES,""+failUnknownProperties);
        KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient,jsonSchemaDeSerializerConfig,HttpRequest.class);

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
                HttpRequest.Method.POST,
                HttpRequest.BodyType.BYTE_ARRAY.name()
        );
        httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);

        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));

        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
        Map<String,String> converterConfig= Maps.newHashMap();
        converterConfig.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        converterConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpRequest.class.getName());
        jsonSchemaConverter.configure(converterConfig,false);

        //when
        HttpRequestAsStruct httpRequestAsStruct = new HttpRequestAsStruct(httpRequest);
        byte[] fromConnectData = jsonSchemaConverter.fromConnectData(DUMMY_TOPIC, HttpRequestAsStruct.SCHEMA, httpRequestAsStruct.toStruct());
        //like in kafka connect Sink connector, convert byte[] to struct
        SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(DUMMY_TOPIC, fromConnectData);
        //then
        Schema schema = schemaAndValue.schema();
        assertThat(schema).isEqualTo(HttpRequestAsStruct.SCHEMA);
        assertThat(schemaAndValue.value()).isEqualTo(httpRequestAsStruct.toStruct());
    }



    @Test
    void test_serialize_and_deserialize_http_request_with_body_as_byte_array() throws IOException {
        //given

        //build httpRequest
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                HttpRequest.Method.POST,
                HttpRequest.BodyType.BYTE_ARRAY.name()
        );
        httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);

        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        boolean useOneOfForNullables=false;
        boolean failUnknownProperties=false;


        //build serializer
        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,""+useOneOfForNullables);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,""+failUnknownProperties);
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));
        KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);

        //when
        //serialize http as byte[]
        byte[] bytes = serializer.serialize(DUMMY_TOPIC, httpRequest);

        System.out.println("bytesAsString:"+new String(bytes, StandardCharsets.UTF_8));


        //build serializer
        Map<String,String> jsonSchemaDeSerializerConfig = Maps.newHashMap();
        jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpRequest.class.getClass().getName());
        jsonSchemaDeSerializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES,""+failUnknownProperties);
        KafkaJsonSchemaDeserializer<HttpRequest> deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient,jsonSchemaDeSerializerConfig,HttpRequest.class);

        HttpRequest deserializedRequest = deserializer.deserialize(DUMMY_TOPIC, bytes);

        //then
        assertThat(deserializedRequest).isEqualTo(httpRequest);

    }



    @Test
    void test_with_empty_struct(){
        //given
        Struct struct = new Struct(HttpRequestAsStruct.SCHEMA);
        //when
        Assertions.assertThrows(NullPointerException.class,()->HttpRequestAsStruct.Builder.anHttpRequest().withStruct(struct).build());
    }
    @Test
    void test_with_struct_only_url(){
        //given
        Struct struct = new Struct(HttpRequestAsStruct.SCHEMA);
        struct.put("url","http://stuff.com");
        //when
        Assertions.assertThrows(NullPointerException.class,()->HttpRequestAsStruct.Builder.anHttpRequest().withStruct(struct).build());
    }
    @Test
    void test_with_struct_only_url_and_method(){
        //given
        Struct struct = new Struct(HttpRequestAsStruct.SCHEMA);
        struct.put("url","http://stuff.com");
        struct.put("method","GET");
        //when
        Assertions.assertThrows(NullPointerException.class,()->HttpRequestAsStruct.Builder.anHttpRequest().withStruct(struct).build());
    }
    @Test
    void test_with_struct_nominal_case(){
        //given
        Struct struct = new Struct(HttpRequestAsStruct.SCHEMA);
        String dummyUrl = "http://stuff.com";
        struct.put("url", dummyUrl);
        HttpRequest.Method dummyMethod = HttpRequest.Method.GET;
        struct.put("method", dummyMethod.name());
        String dummyBodyType = "STRING";
        struct.put("bodyType", dummyBodyType);
        struct.put("bodyAsString", DUMMY_BODY_AS_STRING);
        //when
        HttpRequest httpRequest = HttpRequestAsStruct.Builder.anHttpRequest().withStruct(struct).build();
        //then
        assertThat(httpRequest).isNotNull();
        assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
        assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
        assertThat(httpRequest.getBodyType()).hasToString(dummyBodyType);
        assertThat(httpRequest.getBodyAsString()).hasToString(DUMMY_BODY_AS_STRING);
    }
    @Test
    void test_with_struct_and_byte_array_nominal_case(){
        //given
        Struct struct = new Struct(HttpRequestAsStruct.SCHEMA);
        String dummyUrl = "http://stuff.com";
        struct.put("url", dummyUrl);
        HttpRequest.Method dummyMethod = HttpRequest.Method.POST;
        struct.put("method", dummyMethod.name());
        String dummyBodyType = "BYTE_ARRAY";
        struct.put("bodyType", dummyBodyType);
        struct.put("bodyAsByteArray", Base64.getEncoder().encodeToString(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8)));
        //when
        HttpRequest httpRequest = HttpRequestAsStruct.Builder.anHttpRequest().withStruct(struct).build();
        //then
        assertThat(httpRequest).isNotNull();
        assertThat(httpRequest.getUrl()).isEqualTo(dummyUrl);
        assertThat(httpRequest.getMethod()).isEqualTo(dummyMethod);
        assertThat(httpRequest.getBodyType()).hasToString(dummyBodyType);
        assertThat(httpRequest.getBodyAsByteArray()).isEqualTo(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
    }


    @Test
    void validate_schema_with_JsonSchemaProvider(){
        JsonSchemaProvider jsonSchemaProvider = new JsonSchemaProvider();
        Optional<ParsedSchema> parsedSchema = jsonSchemaProvider.parseSchema(HttpRequest.SCHEMA_AS_STRING, Lists.newArrayList());
        assertThat(parsedSchema).isPresent();
        parsedSchema.get().validate(true);
    }

    @Test
    void validate_schema_with_AvroJsonSchemaProvider(){
        AvroSchemaProvider avroSchemaProviderSchemaProvider = new AvroSchemaProvider();
        Optional<ParsedSchema> parsedSchema = avroSchemaProviderSchemaProvider.parseSchema(HttpRequest.SCHEMA_AS_STRING, Lists.newArrayList());
        assertThat(parsedSchema).isNotPresent();
    }

    @Test
    void get_http_request_jsonschema(){
        JsonSchemaConfig jsonSchemaConfig = getConfig(false, false);
        jsonSchemaConfig = jsonSchemaConfig.withJsonSchemaDraft(JsonSchemaDraft.DRAFT_2019_09);
        ObjectMapper objectMapper = new JsonMapper();
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, jsonSchemaConfig);
        JsonNode jsonSchema = jsonSchemaGenerator.generateJsonSchema(HttpRequest.class);
        JsonSchema schema = new JsonSchema(jsonSchema);
        String schemaAsString = schema.toString();
        assertThat(schemaAsString).isNotNull();
    }

    /**
     * from JsonSchemaUtils...
     * @param useOneofForNullables
     * @param failUnknownProperties
     * @return
     */
    private static JsonSchemaConfig getConfig(
            boolean useOneofForNullables, boolean failUnknownProperties) {
        final JsonSchemaConfig vanilla = JsonSchemaConfig.vanillaJsonSchemaDraft4();
        return JsonSchemaConfig.create(
                vanilla.autoGenerateTitleForProperties(),
                Optional.empty(),
                true,
                useOneofForNullables,
                vanilla.usePropertyOrdering(),
                vanilla.hidePolymorphismTypeProperty(),
                vanilla.disableWarnings(),
                vanilla.useMinLengthForNotNull(),
                vanilla.useTypeIdForDefinitionName(),
                Collections.emptyMap(),
                vanilla.useMultipleEditorSelectViaProperty(),
                Collections.emptySet(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                vanilla.subclassesResolver(),
                failUnknownProperties,
                null
        );
    }
}