package io.github.clescot.kafka.connect.http.core;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.connect.data.Struct;
import org.json.JSONException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.nio.charset.StandardCharsets;

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
            String expectedHttpPart =
                    "{\n" +
                    "  \"bodyType\": \"BYTE_ARRAY\",\n" +
                    "  \"headers\": {\n" +
                    "    \"Content-Type\": [\n" +
                    "      \"application/octet-stream\"\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  \"contentAsByteArray\": \"dGVzdA==\"\n" +
                    "}";
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
                    "{\n" +
                    "  \"bodyType\": \"BYTE_ARRAY\",\n" +
                    "  \"headers\": {\n" +
                    "    \"Content-Type\": [\n" +
                    "      \"application/json\"\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  \"contentAsByteArray\": \"dGVzdA==\"\n" +
                    "}";
            String serializedHttpPart = objectMapper.writeValueAsString(httpPart);
            JSONAssert.assertEquals(expectedHttpPart, serializedHttpPart,true);
        }
        @Test
        void test_serialization_content_as_string() throws JsonProcessingException, JSONException {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());

            HttpPart httpPart = new HttpPart("test");
            String expectedHttpPart =
                    "{\n" +
                    "  \"bodyType\": \"STRING\",\n" +
                    "  \"headers\": {\n" +
                    "    \"Content-Type\": [\n" +
                    "      \"application/json\"\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  \"contentAsString\": \"test\"\n" +
                    "}";
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
                    "{\n" +
                    "  \"bodyType\": \"STRING\",\n" +
                    "  \"headers\": {\n" +
                    "    \"Content-Type\": [\n" +
                    "      \"application/toto\"\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  \"contentAsString\": \"test\"\n" +
                    "}";
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
                    "{\n" +
                    "  \"bodyType\": \"BYTE_ARRAY\",\n" +
                    "  \"headers\": {\n" +
                    "    \"Content-Type\": [\n" +
                    "      \"application/octet-stream\"\n" +
                    "    ]\n" +
                    "  },\n" +
                    "  \"contentAsByteArray\": \"dGVzdA==\"\n" +
                    "}";
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
            assertDoesNotThrow(()->new HttpPart("parameterName","parameterValue", Objects.requireNonNull(url).toURI()));
        }
        @Test
        void test_constructor_with_file_as_ref_content_and_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart(headers,"parameterName","parameterValue", Objects.requireNonNull(url).toURI()));
        }

        @Test
        void test_constructor_with_file_as_content(){
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart("parameterName","parameterValue",new File(Objects.requireNonNull(url).toURI())));
        }
        @Test
        void test_constructor_with_file_as_content_and_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart(headers,"parameterName","parameterValue",new File(Objects.requireNonNull(url).toURI())));
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
            assertThat(clonedHttpPart).isEqualTo(httpPart);
            assertThat(clonedHttpPart).isEqualTo(httpPart);
            assertThat(clonedHttpPart).isNotSameAs(httpPart);
            assertThat(clonedHttpPart.hashCode()).isEqualTo(httpPart.hashCode());
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }

        @Test
        void test_clone_content_as_form_entry_with_file() throws CloneNotSupportedException {
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            HttpPart httpPart = new HttpPart(headers,"parameterName","parameterValue",new File("src/test/resources/upload.txt"));
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart.hashCode()).isEqualTo(httpPart.hashCode());
            assertThat(clonedHttpPart).isEqualTo(httpPart);
            assertThat(clonedHttpPart).isNotSameAs(httpPart);
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }

        @Test
        void test_clone_content_as_form_entry_with_file_uri() throws CloneNotSupportedException, URISyntaxException {
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            HttpPart httpPart = new HttpPart(headers,"parameterName","parameterValue",new URI("src/test/resources/upload.txt"));
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart.hashCode()).isEqualTo(httpPart.hashCode());
            assertThat(clonedHttpPart).isEqualTo(httpPart);
            assertThat(clonedHttpPart).isNotSameAs(httpPart);
            assertThat(clonedHttpPart.getContentType()).isEqualTo(httpPart.getContentType());
            assertThat(clonedHttpPart.getBodyType()).isEqualTo(httpPart.getBodyType());
        }
        @Test
        void test_clone_content_as_string() throws CloneNotSupportedException {
            HttpPart httpPart = new HttpPart("test");
            HttpPart clonedHttpPart = (HttpPart) httpPart.clone();
            assertThat(clonedHttpPart.hashCode()).isEqualTo(httpPart.hashCode());
            assertThat(clonedHttpPart).isEqualTo(httpPart);
            assertThat(clonedHttpPart).isNotSameAs(httpPart);
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
            assertThat(clonedHttpPart.hashCode()).isEqualTo(httpPart.hashCode());
            assertThat(clonedHttpPart).isEqualTo(httpPart);
            assertThat(clonedHttpPart).isNotSameAs(httpPart);
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
            assertThat(httpPart.toString()).isEqualTo(expected);
        }

        @Test
        void test_to_string_content_as_string() {
            HttpPart httpPart = new HttpPart("test");
            String expected = "HttpPart{bodyType:\"STRING\", headers:{Content-Type=[application/json]}, \"contentAsString\":test\", \"contentAsByteArray\":\"null\", \"contentAsForm\":\"null\", \"fileUri\":\"null\"}";
            assertThat(httpPart.toString()).isEqualTo(expected);
        }
    }
}