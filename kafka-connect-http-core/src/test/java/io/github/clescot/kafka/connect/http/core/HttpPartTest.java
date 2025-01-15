package io.github.clescot.kafka.connect.http.core;

import java.net.URL;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class HttpPartTest {

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