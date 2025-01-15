package io.github.clescot.kafka.connect.http.core;

import java.net.URL;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.json.JSONException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

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
            String expectedHttpPart = "" +
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
            String expectedHttpPart = "" +
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
            String expectedHttpPart = "" +
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
            String expectedHttpPart = "" +
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
            assertDoesNotThrow(()->new HttpPart("parameterName","parameterValue",url.toURI()));
        }
        @Test
        void test_constructor_with_file_as_ref_content_and_headers(){
            Map<String,List<String>> headers = new HashMap<>();
            headers.put("Content-Type",List.of("application/json"));
            URL url = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            assertDoesNotThrow(()->new HttpPart(headers,"parameterName","parameterValue",url.toURI()));
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
}