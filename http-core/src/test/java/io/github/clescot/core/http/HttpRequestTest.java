package io.github.clescot.core.http;

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
import java.util.*;

import static io.github.clescot.core.http.HttpRequest.BODY_AS_BYTE_ARRAY_FIELD;
import static io.github.clescot.core.http.MediaType.APPLICATION_X_WWW_FORM_URLENCODED;
import static io.github.clescot.core.http.SchemaLoader.*;
import static org.assertj.core.api.Assertions.assertThat;

class HttpRequestTest {


    private static final String DUMMY_BODY_AS_STRING = "stuff";

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
            HttpRequest clonedHttpRequest = (HttpRequest) httpRequest.clone();
            //then
            assertThat(clonedHttpRequest).isEqualTo(httpRequest);
        }

        @Test
        void test_clone_with_body_as_string_and_attributes() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            HashMap<String, Object> attributes = Maps.newHashMap();
            attributes.put("attr1","value1");
            attributes.put("attr2","value2");
            httpRequest.setAttributes(attributes);
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);
            //when
            HttpRequest clonedHttpRequest = (HttpRequest) httpRequest.clone();
            //then
            assertThat(clonedHttpRequest).isEqualTo(httpRequest);
        }

        @Test
        void test_clone_with_body_as_string_and_attributes_with_different_object() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            HashMap<String, Object> attributes = Maps.newHashMap();
            attributes.put("attr1","value1");
            attributes.put("attr2","value2");
            httpRequest.setAttributes(attributes);
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);
            //when
            HttpRequest clonedHttpRequest = (HttpRequest) httpRequest.clone();
            //then
            HttpRequest httpRequest2 = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            HashMap<String, Object> attributes2 = Maps.newHashMap();
            attributes2.put("attr1","value1");
            //atttributes2 attr2 has different value
            attributes2.put("attr2","value3");
            httpRequest2.setAttributes(attributes2);
            httpRequest2.setBodyAsString(DUMMY_BODY_AS_STRING);
            Map<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers2.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers2.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest2.setHeaders(headers2);
            assertThat(clonedHttpRequest).isNotEqualTo(httpRequest2);
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
            HttpRequest clonedHttpRequest = (HttpRequest) httpRequest.clone();
            //then
            assertThat(clonedHttpRequest).isEqualTo(httpRequest);
        }
    }

    @Nested
    class TestEqualsAndHashCode {

        @Test
        void test_with_null(){
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
            assertThat(httpRequest).isNotNull();
        }

        @Test
        void test_with_other_class(){
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
            assertThat(httpRequest).isNotEqualTo(new ArrayList<>());
        }


        @Test
        void test_same_instance(){
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
            assertThat(httpRequest)
                    .isEqualTo(httpRequest)
                    .hasSameHashCodeAs(httpRequest);
        }

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
            assertThat(httpRequest1)
                    .isEqualTo(httpRequest2)
                    .hasSameHashCodeAs(httpRequest2);

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
            assertThat(httpRequest1)
                    .isEqualTo(httpRequest2)
                    .hasSameHashCodeAs(httpRequest2);

        }
    }

    @Nested
    class TestGetHeadersLength{
        @Test
        void test_get_headers_length_with_no_headers() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            //when
            long headersLength = httpRequest.getHeadersLength();
            //then
            assertThat(headersLength).isZero();
        }

        @Test
        void test_get_headers_length_with_headers() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            Map<String, List<String>> headers = Maps.newHashMap();
            String key1 = "X-stuff";
            String value1 = "m-y-value";
            headers.put(key1, Lists.newArrayList(value1));
            String key2 = "X-correlation-id";
            String value2 = "44-999-33-dd";
            headers.put(key2, Lists.newArrayList(value2,value2));
            String key3 = "X-request-id";
            String value3 = "11-999-ff-777";
            headers.put(key3, Lists.newArrayList(value3));
            httpRequest.setHeaders(headers);
            //when
            long headersLength = httpRequest.getHeadersLength();
            //then
            assertThat(headersLength).isEqualTo(
                    key1.length()+value1.length()+
                            key2.length()+value2.length()*2+
                            key3.length()+value3.length());
        }

    }

    @Nested
    class TestBodyGetContentType {

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
        @Test
        void test_get_content_type_with_headers_and_empty_content_type_and_multipart() {
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
            Map<String,HttpPart> parts = Maps.newHashMap();
            HttpPart part1 = new HttpPart("part1".getBytes(StandardCharsets.UTF_8));
            parts.put("part1",part1);
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            parts.put("part2",part2);
            HttpPart part3 = new HttpPart("part3".getBytes(StandardCharsets.UTF_8));
            parts.put("part3",part3);
            httpRequest.setParts(parts);
            assertThat(httpRequest.getContentType()).startsWith("multipart/form-data; boundary=");

        }
    }


    @Nested
    class TestGetLength{
        @Test
        void test_get_length_with_no_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            //when
            long length = httpRequest.getLength();
            //then
            assertThat(length).isZero();
        }

        @Test
        void test_get_length_with_body_as_string() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest.setBodyAsString(DUMMY_BODY_AS_STRING);
            //when
            long length = httpRequest.getLength();
            //then
            assertThat(length).isEqualTo(DUMMY_BODY_AS_STRING.length());
        }

        @Test
        void test_get_length_with_body_as_byte_array() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            httpRequest.setBodyAsByteArray(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8));
            //when
            long length = httpRequest.getLength();
            //then
            long headersLength = httpRequest.getHeadersLength();
            assertThat(length).isEqualTo(headersLength+DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8).length);
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

    @Nested
    class TestToString{
        @Test
        void test_to_string_with_body_as_string() {
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
            String toString = httpRequest.toString();
            //then
            assertThat(toString).contains("url='http://www.stuff.com'")
                    .contains("method=GET")
                    .contains("bodyType=STRING")
                    .contains("bodyAsString='" + DUMMY_BODY_AS_STRING+"'");
        }
        @Test
        void test_to_string_with_body_as_string_and_attributes() {
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
            Map<String,Object> attributes = Maps.newHashMap();
            attributes.put("attr1","value1");
            attributes.put("attr2","value2");
            httpRequest.setAttributes(attributes);
            httpRequest.setHeaders(headers);
            //when
            String toString = httpRequest.toString();
            //then
            assertThat(toString).contains("url='http://www.stuff.com'")
                    .contains("attributes='{attr2=value2, attr1=value1}'")
                    .contains("method=GET")
                    .contains("bodyType=STRING")
                    .contains("bodyAsString='" + DUMMY_BODY_AS_STRING+"'");
        }

        @Test
        void test_to_string_with_body_as_byte_array() {
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
            httpRequest.setHeaders(headers);
            //when
            String toString = httpRequest.toString();
            //then
            assertThat(toString)
                    .contains("url='http://www.stuff.com'")
                    .contains("method=GET")
                    .contains("bodyType=BYTE_ARRAY");
        }
    }

    @Nested
    class TestGetContentLength{
        @Test
        void test_get_content_length_with_string_body() {
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
            long contentLength = httpRequest.getBodyContentLength();
            //then
            assertThat(contentLength).isEqualTo(DUMMY_BODY_AS_STRING.length());
        }

        @Test
        void test_get_content_length_with_byte_array_body() {
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
            httpRequest.setHeaders(headers);
            //when
            long contentLength = httpRequest.getBodyContentLength();
            //then
            assertThat(contentLength).isEqualTo(DUMMY_BODY_AS_STRING.getBytes(StandardCharsets.UTF_8).length);
        }

        @Test
        void test_get_content_length_without_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            //when
            long contentLength = httpRequest.getBodyContentLength();
            //then
            assertThat(contentLength).isZero();
        }

        @Test
        void test_get_content_length_with_body_as_form() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            Map<String, String> form = Maps.newHashMap();
            String value1 = "value1";
            String key1 = "key1";
            form.put(key1, value1);
            String value2 = "value2";
            String key2 = "key22222";
            form.put(key2, value2);
            httpRequest.setBodyAsForm(form);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);
            //when
            long contentLength = httpRequest.getBodyContentLength();
            //then
            assertThat(contentLength).isGreaterThan(0L) //depends on the form encoding
                                     .isEqualTo(key1.length()+value1.length()+key2.length()+value2.length()); //depends on the form encoding
        }

        @Test
        void test_get_content_length_with_multipart_body() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.POST
            );
            Map<String,HttpPart> parts = Maps.newHashMap();
            HttpPart part1 = new HttpPart("part1".getBytes(StandardCharsets.UTF_8));
            parts.put("part1",part1);
            HttpPart part2 = new HttpPart("part2".getBytes(StandardCharsets.UTF_8));
            parts.put("part2",part2);
            HttpPart part3 = new HttpPart("part3".getBytes(StandardCharsets.UTF_8));
            parts.put("part3",part3);
            httpRequest.setParts(parts);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("X-stuff", Lists.newArrayList("m-y-value"));
            headers.put("X-correlation-id", Lists.newArrayList("44-999-33-dd"));
            headers.put("X-request-id", Lists.newArrayList("11-999-ff-777"));
            httpRequest.setHeaders(headers);
            //when
            long contentLength = httpRequest.getBodyContentLength();
            //then
            assertThat(contentLength)
                    .isGreaterThan(0L)
                    .isEqualTo(part1.getBodyContentLength() + part2.getBodyContentLength() + part3.getBodyContentLength());
        }
    }
}