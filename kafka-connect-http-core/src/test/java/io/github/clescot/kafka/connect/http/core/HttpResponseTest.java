package io.github.clescot.kafka.connect.http.core;

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
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static io.github.clescot.kafka.connect.http.core.SchemaLoader.*;
import static org.assertj.core.api.Assertions.assertThat;

class HttpResponseTest {

    private KafkaJsonSchemaSerializer<HttpResponse> serializer;
    private KafkaJsonSchemaDeserializer<HttpResponse> deserializer;
    private static final String RESPONSE_TOPIC = "dummy_response";
    private static final String REQUEST_TOPIC = "dummy_request";
    private static final String EXCHANGE_TOPIC = "dummy_exchange";
    @BeforeEach
    public void setup() throws RestClientException, IOException {
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
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE,HttpResponse.class.getName());
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA,"true");
        jsonSchemaDeserializerConfig.put(KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES,""+true);
        deserializer = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient,jsonSchemaDeserializerConfig,HttpResponse.class);
    }

    @Nested
    class TestSerialize{
        @Test
        public void test_serialize_empty_http_response(){
            HttpResponse httpResponse = new HttpResponse();
            //required fields are missing
            Assertions.assertThrows(SerializationException.class,()->serializer.serialize("dummy_topic",httpResponse));
        }

        @Test
        public void test_serialize_http_response_with_required_fields(){
            HttpResponse httpResponse = new HttpResponse();
            httpResponse.setStatusCode(200);
            httpResponse.setStatusMessage("OK");
            //required fields are missing
            byte[] bytes = serializer.serialize(RESPONSE_TOPIC, httpResponse);
            assertThat(bytes).isNotEmpty();
            HttpResponse deserializedResponse = deserializer.deserialize(RESPONSE_TOPIC, bytes);
            assertThat(deserializedResponse).isNotNull();
            assertThat(deserializedResponse).isEqualTo(httpResponse);
        }

        @Test
        public void test_serialize_http_response_with_body_as_string(){
            HttpResponse httpResponse = new HttpResponse();
            httpResponse.setStatusCode(200);
            httpResponse.setStatusMessage("OK");
            httpResponse.setBodyAsString("Hello World");
            //required fields are missing

            byte[] bytes = serializer.serialize(RESPONSE_TOPIC, httpResponse);
            assertThat(bytes).isNotEmpty();
            HttpResponse deserializedResponse = deserializer.deserialize(RESPONSE_TOPIC, bytes);
            assertThat(deserializedResponse).isNotNull();
            assertThat(deserializedResponse).isEqualTo(httpResponse);
        }
    }

    @Nested
    class TestClone{
        @Test
        public void test_clone_http_response_with_body_as_string() {
            HttpResponse httpResponse = new HttpResponse(200,"OK");
            httpResponse.setBodyAsString("Hello World");

            HttpResponse cloned = (HttpResponse) httpResponse.clone();

            assertThat(cloned).isNotSameAs(httpResponse);
            assertThat(cloned.getStatusCode()).isEqualTo(httpResponse.getStatusCode());
            assertThat(cloned.getStatusMessage()).isEqualTo(httpResponse.getStatusMessage());
            assertThat(cloned.getHeaders()).containsAllEntriesOf(httpResponse.getHeaders());
            assertThat(cloned.getBodyType()).isEqualTo(httpResponse.getBodyType());
            assertThat(cloned.getBodyAsString()).isEqualTo(httpResponse.getBodyAsString());
            assertThat(cloned.getBodyAsByteArray()).isEqualTo(httpResponse.getBodyAsByteArray());
            assertThat(cloned.getBodyAsForm()).isEqualTo(httpResponse.getBodyAsForm());
        }

        @Test
        public void test_clone_http_response_with_body_as_byte_array() {
            HttpResponse httpResponse = new HttpResponse(200,"OK");
            httpResponse.setBodyAsByteArray("Hello World".getBytes(StandardCharsets.UTF_8));

            HttpResponse cloned = (HttpResponse) httpResponse.clone();

            assertThat(cloned).isNotSameAs(httpResponse);
            assertThat(cloned.getStatusCode()).isEqualTo(httpResponse.getStatusCode());
            assertThat(cloned.getStatusMessage()).isEqualTo(httpResponse.getStatusMessage());
            assertThat(cloned.getHeaders()).containsAllEntriesOf(httpResponse.getHeaders());
            assertThat(cloned.getBodyType()).isEqualTo(httpResponse.getBodyType());
            assertThat(cloned.getBodyAsString()).isEqualTo(httpResponse.getBodyAsString());
            assertThat(cloned.getBodyAsByteArray()).isEqualTo(httpResponse.getBodyAsByteArray());
            assertThat(cloned.getBodyAsForm()).isEqualTo(httpResponse.getBodyAsForm());
        }

        @Test
        public void test_clone_http_response_with_body_as_form() {
            HttpResponse httpResponse = new HttpResponse(200,"OK");
            Map<String, String> form = Maps.newHashMap();
            form.put("key1", "value1");
            form.put("key2", "value2");
            httpResponse.setBodyAsForm(form);

            HttpResponse cloned = (HttpResponse) httpResponse.clone();

            assertThat(cloned).isNotSameAs(httpResponse);
            assertThat(cloned.getStatusCode()).isEqualTo(httpResponse.getStatusCode());
            assertThat(cloned.getStatusMessage()).isEqualTo(httpResponse.getStatusMessage());
            assertThat(cloned.getHeaders()).containsAllEntriesOf(httpResponse.getHeaders());
            assertThat(cloned.getBodyType()).isEqualTo(httpResponse.getBodyType());
            assertThat(cloned.getBodyAsString()).isEqualTo(httpResponse.getBodyAsString());
            assertThat(cloned.getBodyAsByteArray()).isEqualTo(httpResponse.getBodyAsByteArray());
            assertThat(cloned.getBodyAsForm()).isEqualTo(httpResponse.getBodyAsForm());
        }
    }

    @Nested
    class TestEqualsAndHashCode {
        @Test
        public void test_equals_and_hashcode() {
            HttpResponse response1 = new HttpResponse(200, "OK");
            response1.setBodyAsString("Hello World");

            HttpResponse response2 = new HttpResponse(200, "OK");
            response2.setBodyAsString("Hello World");

            assertThat(response1).isEqualTo(response2);
            assertThat(response1.hashCode()).isEqualTo(response2.hashCode());
        }

        @Test
        public void test_not_equals_different_body_as_string() {
            HttpResponse response1 = new HttpResponse(200, "OK");
            response1.setBodyAsString("Hello World");

            HttpResponse response2 = new HttpResponse(200, "OK");
            response2.setBodyAsString("Hello World2");

            assertThat(response1).isNotEqualTo(response2);
            assertThat(response1.hashCode()).isNotEqualTo(response2.hashCode());
        }

        @Test
        public void test_not_equals_different_status_code() {
            HttpResponse response1 = new HttpResponse(200, "OK");
            HttpResponse response2 = new HttpResponse(404, "Not Found");

            assertThat(response1).isNotEqualTo(response2);
        }
    }

    @Nested
    class TestToStruct{
        @Test
        public void test_toStruct_with_body_as_string() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            httpResponse.setBodyAsString("Hello World");

            var struct = httpResponse.toStruct();
            assertThat(struct.getInt64(HttpResponse.STATUS_CODE)).isEqualTo(200);
            assertThat(struct.getString(HttpResponse.STATUS_MESSAGE)).isEqualTo("OK");
            assertThat(struct.getString(HttpResponse.BODY_AS_STRING)).isEqualTo("Hello World");
        }

        @Test
        public void test_toStruct_with_body_as_byte_array() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            httpResponse.setBodyAsByteArray("Hello World".getBytes(StandardCharsets.UTF_8));

            var struct = httpResponse.toStruct();
            assertThat(struct.getInt64(HttpResponse.STATUS_CODE)).isEqualTo(200);
            assertThat(struct.getString(HttpResponse.STATUS_MESSAGE)).isEqualTo("OK");
            assertThat(struct.getString(HttpResponse.BODY_AS_BYTE_ARRAY)).isEqualTo(Base64.getEncoder().encodeToString("Hello World".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Nested
    class TestGetBodyContentLength {
        @Test
        public void test_getContentLength_without_headers_with_body_as_string() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            httpResponse.setBodyAsString("Hello World");

            long contentLength = httpResponse.getBodyContentLength();
            assertThat(contentLength).isEqualTo("Hello World".getBytes(StandardCharsets.UTF_8).length);
        }

        @Test
        public void test_getContentLength_without_headers_with_body_as_byte_array() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            httpResponse.setBodyAsByteArray("Hello World".getBytes(StandardCharsets.UTF_8));

            long contentLength = httpResponse.getBodyContentLength();
            assertThat(contentLength).isEqualTo("Hello World".getBytes(StandardCharsets.UTF_8).length);
        }

        @Test
        public void test_getContentLength_without_headers_with_body_as_form() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            Map<String, String> form = Maps.newHashMap();
            String key1 = "longkey1";
            String value1 = "valuuuuuue1";
            form.put(key1, value1);
            String key2 = "key2";
            String value2 = "value2";
            form.put(key2, value2);
            httpResponse.setBodyAsForm(form);

            long contentLength = httpResponse.getBodyContentLength();
            assertThat(contentLength).isEqualTo(key1.length()+value1.length()+key2.length()+value2.length()); // Length depends on form encoding
        }

        @Test
        public void test_getContentLength_without_headers_with_multipart() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            Map<String,HttpPart> parts = Maps.newHashMap();
            HttpPart part1 = new HttpPart("part1".getBytes(StandardCharsets.UTF_8));
            parts.put("part1",part1);
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            parts.put("part2",part2);
            HttpPart part3 = new HttpPart("part3".getBytes(StandardCharsets.UTF_8));
            parts.put("part3",part3);
            httpResponse.setParts(parts);
            assertThat(httpResponse.getBodyContentLength()).isEqualTo(
                    part1.getBodyContentLength() + part2.getBodyContentLength() + part3.getBodyContentLength()
            );
        }

        @Test
        public void test_getContentLength_with_headers_with_body_as_form() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            Map<String, String> form = Maps.newHashMap();
            String key1 = "longkey1";
            String value1 = "valuuuuuue1";
            form.put(key1, value1);
            String key2 = "key2";
            String value2 = "value2";
            form.put(key2, value2);
            httpResponse.setBodyAsForm(form);
            Map<String, List<String>> headers = Maps.newHashMap();
            String initialContentLength = "450";
            headers.put("Content-Length", Lists.newArrayList(initialContentLength));
            httpResponse.setHeaders(headers);
            long contentLength = httpResponse.getBodyContentLength();
            assertThat(contentLength).isEqualTo(Long.parseLong(initialContentLength)); // Length depends on form encoding
        }

        @Test
        public void test_getContentLength_with_headers_and_multiple_values_with_body_as_form() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            Map<String, String> form = Maps.newHashMap();
            String key1 = "longkey1";
            String value1 = "valuuuuuue1";
            form.put(key1, value1);
            String key2 = "key2";
            String value2 = "value2";
            form.put(key2, value2);
            httpResponse.setBodyAsForm(form);
            Map<String, List<String>> headers = Maps.newHashMap();
            String initialContentLength = "450";
            String secondContentLength = "800";
            headers.put("Content-Length", Lists.newArrayList(initialContentLength,secondContentLength));
            httpResponse.setHeaders(headers);
            long contentLength = httpResponse.getBodyContentLength();
            assertThat(contentLength).isEqualTo(Long.parseLong(initialContentLength)); // Length depends on form encoding
        }
    }

    @Nested
    class TestGetHeadersLength{
        @Test
        public void test_getHeadersLength_with_empty_headers() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            assertThat(httpResponse.getHeadersLength()).isEqualTo(0);
        }

        @Test
        public void test_getHeadersLength_with_single_header() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList("application/json"));
            httpResponse.setHeaders(headers);
            assertThat(httpResponse.getHeadersLength()).isEqualTo("Content-Type".length() + "application/json".length());
        }

        @Test
        public void test_getHeadersLength_with_multiple_headers() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList("application/json"));
            headers.put("Authorization", Lists.newArrayList("Bearer token"));
            httpResponse.setHeaders(headers);
            assertThat(httpResponse.getHeadersLength()).isEqualTo(
                    "Content-Type".length() + "application/json".length() +
                    "Authorization".length() + "Bearer token".length()
            );
        }
    }

    @Nested
    class TestGetLength{
        @Test
        public void test_getLength_with_empty_response() {
            HttpResponse httpResponse = new HttpResponse();
            assertThat(httpResponse.getLength()).isEqualTo(0);
        }

        @Test
        public void test_getLength_with_body_as_string() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            httpResponse.setBodyAsString("Hello World");
            assertThat(httpResponse.getLength()).isEqualTo(
                    "Hello World".getBytes(StandardCharsets.UTF_8).length +
                    httpResponse.getHeadersLength()
            );
        }

        @Test
        public void test_getLength_with_body_as_byte_array() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            httpResponse.setBodyAsByteArray("Hello World".getBytes(StandardCharsets.UTF_8));
            assertThat(httpResponse.getLength()).isEqualTo(
                    "Hello World".getBytes(StandardCharsets.UTF_8).length +
                    httpResponse.getHeadersLength()
            );
        }

        @Test
        public void test_getLength_with_body_as_form() {
            HttpResponse httpResponse = new HttpResponse(200, "OK");
            Map<String, String> form = Maps.newHashMap();
            form.put("key1", "value1");
            form.put("key2", "value2");
            httpResponse.setBodyAsForm(form);
            assertThat(httpResponse.getLength()).isEqualTo(
                    form
                        .entrySet()
                        .stream()
                        .filter(pair->pair.getValue()!=null)
                        .map(pair->pair.getKey().length()+pair.getValue().length())
                        .reduce(Integer::sum).orElse(0) +
                    httpResponse.getHeadersLength()
            );
        }
    }
}