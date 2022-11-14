package com.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.json.JsonSchemaConverterConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONException;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpRequestTest {


    @Test
    public void test_serialization() throws JsonProcessingException, JSONException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                "GET",
                "STRING",
                "stuff",
                null,
                null
        );
        Map<String,List<String>> headers = Maps.newHashMap();
        headers.put("X-correlation-id",Lists.newArrayList("sfds-55-77"));
        headers.put("X-request-id",Lists.newArrayList("aaaa-4466666-111"));
        httpRequest.setHeaders(headers);

        String expectedHttpRequest = "{\n" +
                "  \"requestId\": null,\n" +
                "  \"correlationId\": null,\n" +
                "  \"timeoutInMs\": null,\n" +
                "  \"retries\": null,\n" +
                "  \"retryDelayInMs\": null,\n" +
                "  \"retryMaxDelayInMs\": null,\n" +
                "  \"retryDelayFactor\": null,\n" +
                "  \"retryJitter\": null,\n" +
                "  \"url\": \"http://www.stuff.com\",\n" +
                "  \"headers\":{\"X-request-id\":[\"aaaa-4466666-111\"],\"X-correlation-id\":[\"sfds-55-77\"]},\n" +
                "  \"method\": \"GET\",\n" +
                "  \"bodyAsString\": \"stuff\",\n" +
                "  \"bodyAsByteArray\": \"\",\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"STRING\"\n" +
                "}";

        String serializedHttpRequest = objectMapper.writeValueAsString(httpRequest);
        JSONAssert.assertEquals(expectedHttpRequest, serializedHttpRequest,true);
    }


    @Test
    public void test_build_http_request_from_struct() throws IOException {
        //given

        //build httpRequest
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                "GET",
                "STRING",
                "stuff",
                null,
                null
        );
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
        headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
        httpRequest.setHeaders(headers);
        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_7;
        boolean useOneOfForNullables=true;
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
        byte[] bytes = serializer.serialize("stuff", httpRequest);

        System.out.println("bytesAsString:"+new String(bytes, StandardCharsets.UTF_8));
        //like in kafka connect Sink connector, convert byte[] to struct
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
        Map<String,String> converterConfig= Maps.newHashMap();
        converterConfig.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaConverter.configure(converterConfig,false);
        SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData("stuff", bytes);
        Struct value = (Struct) schemaAndValue.value();
        assertThat(expectedJsonSchema.equals(value.schema()));
        //when
        HttpRequest parsedHttpRequest = HttpRequest.Builder.anHttpRequest().withStruct(value).build();

        System.out.println(parsedHttpRequest);
    }

}