package io.github.clescot.kafka.connect.http;

import static io.github.clescot.core.http.SchemaLoader.*;
import static io.github.clescot.core.http.SchemaLoader.loadHttpExchangeSchema;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.github.clescot.core.http.HttpResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class HttpResponseAdapterTest {

  private KafkaJsonSchemaSerializer<HttpResponse> serializer;
  private KafkaJsonSchemaDeserializer<HttpResponse> deserializer;
  private static final String RESPONSE_TOPIC = "dummy_response";
  private static final String REQUEST_TOPIC = "dummy_request";
  private static final String EXCHANGE_TOPIC = "dummy_exchange";

  @Nested
  class TestToStruct {

    @BeforeEach
    public void setup() throws RestClientException, IOException {
      SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
      Map<String, String> jsonSchemaSerializerConfig = Maps.newHashMap();
      jsonSchemaSerializerConfig.put(
          KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
      jsonSchemaSerializerConfig.put(
          KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION, jsonSchemaSpecification.toString());
      jsonSchemaSerializerConfig.put(
          KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, "true");
      jsonSchemaSerializerConfig.put(
          KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES, "" + false);
      jsonSchemaSerializerConfig.put(
          KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, "" + true);
      jsonSchemaSerializerConfig.put(
          KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + true);

      MockSchemaRegistryClient schemaRegistryClient =
          new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));
      // Register http part
      ParsedSchema parsedPartSchema = loadHttpPartSchema();
      schemaRegistryClient.register("httpPart" + "-value", parsedPartSchema);
      // register http request
      ParsedSchema parsedHttpRequestSchema = loadHttpRequestSchema();
      schemaRegistryClient.register(REQUEST_TOPIC + "-value", parsedHttpRequestSchema);
      // register http response
      ParsedSchema parsedHttpResponseSchema = loadHttpResponseSchema();
      schemaRegistryClient.register(RESPONSE_TOPIC + "-value", parsedHttpResponseSchema);
      // register http exchange
      ParsedSchema parsedHttpExchangeSchema = loadHttpExchangeSchema();
      schemaRegistryClient.register(EXCHANGE_TOPIC + "-value", parsedHttpExchangeSchema);

      serializer =
          new KafkaJsonSchemaSerializer<>(schemaRegistryClient, jsonSchemaSerializerConfig);
      Map<String, String> jsonSchemaDeserializerConfig = Maps.newHashMap();
      jsonSchemaDeserializerConfig.put(
          KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://stuff.com");
      jsonSchemaDeserializerConfig.put(
          KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, HttpResponse.class.getName());
      jsonSchemaDeserializerConfig.put(
          KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA, "true");
      jsonSchemaDeserializerConfig.put(
          KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES, "" + true);
      deserializer =
          new KafkaJsonSchemaDeserializer<>(
              schemaRegistryClient, jsonSchemaDeserializerConfig, HttpResponse.class);
    }

    @Nested
    class TestSerialize {

      @Test
      public void test_serialize_http_response_with_required_fields() {
        HttpResponse httpResponse = new HttpResponse(200, "OK");
        // required fields are missing
        byte[] bytes = serializer.serialize(RESPONSE_TOPIC, httpResponse);
        assertThat(bytes).isNotEmpty();
        HttpResponse deserializedResponse = deserializer.deserialize(RESPONSE_TOPIC, bytes);
        assertThat(deserializedResponse).isNotNull();
        assertThat(deserializedResponse).isEqualTo(httpResponse);
      }

      @Test
      public void test_serialize_http_response_with_body_as_string() {
        HttpResponse httpResponse = new HttpResponse(200, "OK");
        httpResponse.setBodyAsString("Hello World");
        // required fields are missing

        byte[] bytes = serializer.serialize(RESPONSE_TOPIC, httpResponse);
        assertThat(bytes).isNotEmpty();
        HttpResponse deserializedResponse = deserializer.deserialize(RESPONSE_TOPIC, bytes);
        assertThat(deserializedResponse).isNotNull();
        assertThat(deserializedResponse).isEqualTo(httpResponse);
      }
    }

    @Test
    public void test_toStruct_with_body_as_string() {
      HttpResponse httpResponse = new HttpResponse(200, "OK");
      httpResponse.setBodyAsString("Hello World");

      var struct = HttpResponseAdapter.from(httpResponse).toStruct();

      assertThat(struct.getInt64(HttpResponse.STATUS_CODE_FIELD)).isEqualTo(200);
      assertThat(struct.getString(HttpResponse.STATUS_MESSAGE_FIELD)).isEqualTo("OK");
      assertThat(struct.getString(HttpResponse.BODY_AS_STRING_FIELD)).isEqualTo("Hello World");
    }

    @Test
    public void test_toStruct_with_body_as_byte_array() {
      HttpResponse httpResponse = new HttpResponse(200, "OK");
      httpResponse.setBodyAsByteArray("Hello World".getBytes(StandardCharsets.UTF_8));

      var struct = HttpResponseAdapter.from(httpResponse).toStruct();
      assertThat(struct.getInt64(HttpResponse.STATUS_CODE_FIELD)).isEqualTo(200);
      assertThat(struct.getString(HttpResponse.STATUS_MESSAGE_FIELD)).isEqualTo("OK");
      assertThat(struct.getString(HttpResponse.BODY_AS_BYTE_ARRAY_FIELD))
          .isEqualTo(
              Base64.getEncoder().encodeToString("Hello World".getBytes(StandardCharsets.UTF_8)));
    }
  }
}
