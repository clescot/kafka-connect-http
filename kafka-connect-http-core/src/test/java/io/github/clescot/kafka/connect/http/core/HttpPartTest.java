package io.github.clescot.kafka.connect.http.core;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpPartTest {

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



    }
}