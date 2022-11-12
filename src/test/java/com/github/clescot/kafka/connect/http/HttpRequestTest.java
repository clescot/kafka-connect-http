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
import java.util.List;
import java.util.Map;

class HttpRequestTest {


    @Test
    public void test_serialization() throws JsonProcessingException, JSONException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                "GET",
                "stuff",
                null,
                null
        );

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
                "  \"headers\": {},\n" +
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
    public void test_build_http_request_from_struct_generated_with_bytes_from_json() throws IOException {
        HttpRequest httpRequest = new HttpRequest(
                "http://www.stuff.com",
                "GET",
                "stuff",
                null,
                null
        );
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("X-stuff", Lists.newArrayList("m-y-value"));
        httpRequest.setHeaders(headers);
        JsonSchema schema = JsonSchemaUtils.getSchema(
                httpRequest,
                SpecificationVersion.DRAFT_7,
                true,
                true,
                null
                );
        System.out.println(schema);
        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,SpecificationVersion.DRAFT_7.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,"true");
        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        KafkaJsonSchemaSerializer<HttpRequest> serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);
        byte[] bytes = serializer.serialize("stuff", httpRequest);
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
        Map<String,String> converterConfig= Maps.newHashMap();
        converterConfig.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaConverter.configure(converterConfig,false);
        SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData("stuff", bytes);
        Struct value = (Struct) schemaAndValue.value();
        HttpRequest parsedHttpRequest = HttpRequest.Builder.anHttpRequest().withStruct(value).build();
        System.out.println(parsedHttpRequest);
    }

}