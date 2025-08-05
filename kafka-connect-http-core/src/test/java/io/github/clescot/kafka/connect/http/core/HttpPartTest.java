package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.github.clescot.kafka.connect.http.core.HttpPart.BodyType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class HttpPartTest {


    @Nested
    class TestSerialization{

        @Test
        void test_serialization_content_as_byte_array() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());

            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            String expectedHttpPart = """
                    {
                      "bodyType": "BYTE_ARRAY",
                      "headers": {
                        "Content-Type": [
                          "application/octet-stream"
                        ]
                      },
                      "contentAsByteArray": "dGVzdA=="
                    }
                    """;
            String serializedHttpPart = objectMapper.writeValueAsString(httpPart);
            JSONAssert.assertEquals(expectedHttpPart, serializedHttpPart,true);
        }
        @Test
        void test_serialization_content_as_byte_array_and_headers() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            HttpPart httpPart = new HttpPart(headers, "test".getBytes(StandardCharsets.UTF_8));
            String expectedHttpPart =
                    """
                    {
                      "bodyType": "BYTE_ARRAY",
                      "headers": {
                        "Content-Type": [
                          "application/json"
                        ]
                      },
                      "contentAsByteArray": "dGVzdA=="
                    }""";
            String serializedHttpPart = objectMapper.writeValueAsString(httpPart);
            JSONAssert.assertEquals(expectedHttpPart, serializedHttpPart,true);
        }
        @Test
        void test_serialization_content_as_string() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());

            HttpPart httpPart = new HttpPart("test");
            String expectedHttpPart =
                    """
                    {
                      "bodyType": "STRING",
                      "headers": {
                        "Content-Type": [
                          "application/json"
                        ]
                      },
                      "contentAsString": "test"
                    }""";
            String serializedHttpPart = objectMapper.writeValueAsString(httpPart);
            JSONAssert.assertEquals(expectedHttpPart, serializedHttpPart,true);
        }
        @Test
        void test_serialization_content_as_string_and_headers() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/toto"));
            HttpPart httpPart = new HttpPart(headers, "test");
            String expectedHttpPart =
                    """
                    {
                      "bodyType": "STRING",
                      "headers": {
                        "Content-Type": [
                          "application/toto"
                        ]
                      },
                      "contentAsString": "test"
                    }""";
            String serializedHttpPart = objectMapper.writeValueAsString(httpPart);
            JSONAssert.assertEquals(expectedHttpPart, serializedHttpPart,true);
        }
    }

    @Nested
    class TestDeserialization{


        @Test
        void test_deserialization_content_as_byte_array() throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            String serializedHttpPart =
                    """
                    {
                      "bodyType": "BYTE_ARRAY",
                      "headers": {
                        "Content-Type": [
                          "application/octet-stream"
                        ]
                      },
                      "contentAsByteArray": "dGVzdA=="
                    }
                    """;
            HttpPart httpPart = objectMapper.readValue(serializedHttpPart,HttpPart.class);
            assertThat(httpPart).isEqualTo(new HttpPart("test".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Nested
    class TestConstructor{

        @Test
        void test_constructor_with_byte_array(){
            assertDoesNotThrow(()->new HttpPart("test".getBytes(StandardCharsets.UTF_8)));
        }
        @Test
        void test_constructor_with_byte_array_and_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            assertDoesNotThrow(()->new HttpPart(headers,"test".getBytes(StandardCharsets.UTF_8)));
        }
        @Test
        void test_constructor_with_string_content(){
            assertDoesNotThrow(()->new HttpPart("test"));
        }
        @Test
        void test_constructor_with_string_content_and_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            assertDoesNotThrow(()->new HttpPart(headers,"test"));
        }
        @Test
        void test_constructor_with_file_as_ref_content(){
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart("filename", Objects.requireNonNull(url).toURI()));
        }
        @Test
        void test_constructor_with_file_as_ref_content_and_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart(headers,"filename", Objects.requireNonNull(url).toURI()));
        }

        @Test
        void test_constructor_with_file_as_content(){
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart("filename",new File(Objects.requireNonNull(url).toURI())));
        }
        @Test
        void test_constructor_with_file_as_content_and_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart(headers,"filename",new File(Objects.requireNonNull(url).toURI())));
        }
        @Test
        void test_constructor_with_struct_and_body_as_string(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            Struct struct = new Struct(HttpPart.SCHEMA);
            struct.put("headers",headers);
            struct.put("bodyType",STRING.toString());
            struct.put("bodyAsString","dummy string");
            HttpPart httpPart = new HttpPart(struct);
            assertThat(httpPart.getBodyType()).isEqualTo(STRING);
            assertThat(httpPart.getContentAsString()).isEqualTo("dummy string");
        }
    }

    @Nested
    class TestToStruct{
        @Test
        void test_to_struct_content_as_byte_array() {
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            Struct struct = httpPart.toStruct();
            assertThat(struct.getString("bodyType")).isEqualTo(HttpPart.BodyType.BYTE_ARRAY.toString());
            assertThat(new String(Base64.getDecoder().decode(struct.getString("bodyAsByteArray")))).isEqualTo("test");
        }

        @Test
        void test_to_struct_content_as_string() {
            HttpPart httpPart = new HttpPart("test");
            Struct struct = httpPart.toStruct();
            assertThat(struct.getString("bodyType")).isEqualTo(HttpPart.BodyType.STRING.toString());
            assertThat(struct.getString("bodyAsString")).isEqualTo("test");
        }
    }


    @Nested
    class TestEquals{

        @Test
        void test_equals_content_as_byte_array(){
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            HttpPart httpPart2 = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            assertThat(httpPart).isEqualTo(httpPart2);
        }

        @Test
        void test_equals_content_as_byte_array_different_content(){
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            HttpPart httpPart2 = new HttpPart("test2".getBytes(StandardCharsets.UTF_8));
            assertThat(httpPart).isNotEqualTo(httpPart2);
        }

        @Test
        void test_equals_content_as_byte_array_with_same_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            HttpPart httpPart = new HttpPart(headers,"test".getBytes(StandardCharsets.UTF_8));
            HttpPart httpPart2 = new HttpPart(headers,"test".getBytes(StandardCharsets.UTF_8));
            assertThat(httpPart).isEqualTo(httpPart2);
        }

        @Test
        void test_equals_content_as_byte_array_with_different_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            HttpPart httpPart = new HttpPart(headers,"test".getBytes(StandardCharsets.UTF_8));
            Map<String,List<String>> headers2 = new HashMap<>();
            headers.put("Content-Type2",List.of("application/json"));
            HttpPart httpPart2 = new HttpPart(headers2,"test".getBytes(StandardCharsets.UTF_8));
            assertThat(httpPart).isNotEqualTo(httpPart2);
        }

        @Test
        void test_equals_content_as_String(){
            HttpPart httpPart = new HttpPart("test");
            HttpPart httpPart2 = new HttpPart("test");
            assertThat(httpPart).isEqualTo(httpPart2);
        }

        @Test
        void test_equals_content_as_string_with_same_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            HttpPart httpPart = new HttpPart(headers,"test");
            HttpPart httpPart2 = new HttpPart(headers,"test");
            assertThat(httpPart).isEqualTo(httpPart2);
        }
    }

    @Nested
    class TestClone{
        @Test
        void test_clone_content_as_byte_array() throws CloneNotSupportedException {
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart)
                    .isEqualTo(httpPart)
                    .isNotSameAs(httpPart)
                    .hasSameHashCodeAs(httpPart);
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }

        @Test
        void test_clone_content_as_form_entry_with_file() throws CloneNotSupportedException {
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            HttpPart httpPart = new HttpPart(headers,"filename",new File("src/test/resources/upload.txt"));
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart)
                    .hasSameHashCodeAs(httpPart)
                    .isEqualTo(httpPart)
                    .isNotSameAs(httpPart);
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }

        @Test
        void test_clone_content_as_form_entry_with_file_uri() throws CloneNotSupportedException, URISyntaxException {
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            URL uploadUrl = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            Assertions.assertNotNull(uploadUrl);
            HttpPart httpPart = new HttpPart(headers,"filename",uploadUrl.toURI());
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart)
                    .hasSameHashCodeAs(httpPart)
                    .isEqualTo(httpPart)
                    .isNotSameAs(httpPart);
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }
        @Test
        void test_clone_content_as_string() throws CloneNotSupportedException {
            HttpPart httpPart = new HttpPart("test");
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart).hasSameHashCodeAs(httpPart)
                    .isEqualTo(httpPart)
                    .isNotSameAs(httpPart);
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }
        @Test
        void test_clone_content_as_string_with_headers() throws CloneNotSupportedException {
            HttpPart httpPart = new HttpPart("test");
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            httpPart.setHeaders(headers);
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart)
                    .hasSameHashCodeAs(httpPart)
                    .isEqualTo(httpPart)
                    .isNotSameAs(httpPart);
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }
    }

    @Nested
    class TestToString {

        @Test
        void test_to_string_content_as_byte_array() {
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            String expected = "HttpPart{bodyType:\"BYTE_ARRAY\", headers:{Content-Type=[application/octet-stream]}, \"contentAsString\":null\", \"contentAsByteArray\":\"dGVzdA==\", \"contentAsForm\":\"null\", \"fileUri\":\"null\"}";
            assertThat(httpPart).hasToString(expected);
        }

        @Test
        void test_to_string_content_as_string() {
            HttpPart httpPart = new HttpPart("test");
            String expected = "HttpPart{bodyType:\"STRING\", headers:{Content-Type=[application/json]}, \"contentAsString\":test\", \"contentAsByteArray\":\"\", \"contentAsForm\":\"null\", \"fileUri\":\"null\"}";
            assertThat(httpPart).hasToString(expected);
        }
    }

    @Nested
    class TestGetBodyLength{
        @Test
        void test_get_body_length_content_as_byte_array() {
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            long bodyLength = httpPart.getBodyContentLength();
            assertThat(bodyLength).isEqualTo("test".getBytes(StandardCharsets.UTF_8).length);
        }

        @Test
        void test_get_body_length_content_as_string() {
            HttpPart httpPart = new HttpPart("test");
            long bodyLength = httpPart.getBodyContentLength();
            assertThat(bodyLength).isEqualTo("test".length());
        }

        @Test
        void test_get_body_length_content_as_form_entry_with_file() throws URISyntaxException {
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            HttpPart httpPart = new HttpPart("filename", Objects.requireNonNull(url).toURI());
            long bodyLength = httpPart.getBodyContentLength();
            assertThat(bodyLength).isEqualTo(new File(Objects.requireNonNull(url).toURI()).length());
        }

        @Test
        void test_get_body_length_content_as_form_entry_with_file_uri() throws URISyntaxException {
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            HttpPart httpPart = new HttpPart("filename", Objects.requireNonNull(url).toURI());
            long bodyLength = httpPart.getBodyContentLength();
            assertThat(bodyLength).isEqualTo(new File(Objects.requireNonNull(url).toURI()).length());
        }

    }

    @Nested
    class TestGetHeadersLength{
        @Test
        void test_get_headers_length_with_no_headers() {
            //given
            HttpPart httpPart = new HttpPart(
                    "test"
            );
            //when
            long headersLength = httpPart.getHeadersLength();
            //then
            assertThat(httpPart.getContentType()).isEqualTo("application/json");
            assertThat(headersLength).isEqualTo("Content-TYpe".length() + "application/json".length());
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

        @Test
        void test_get_headers_length_without_headers() {
            //given
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.stuff.com",
                    HttpRequest.Method.GET
            );
            //when
            long headersLength = httpRequest.getHeadersLength();
            //then
            assertThat(headersLength).isEqualTo(0);
        }
    }

    @Nested
    class TestGetLength{
        @Test
        void test_get_length_content_as_byte_array() {
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            long length = httpPart.getLength();
            assertThat(length).isEqualTo("test".getBytes(StandardCharsets.UTF_8).length + httpPart.getHeadersLength());
        }

        @Test
        void test_get_length_content_as_string() {
            HttpPart httpPart = new HttpPart("test");
            long length = httpPart.getLength();
            assertThat(length).isEqualTo("test".length() + httpPart.getHeadersLength());
        }
    }
}