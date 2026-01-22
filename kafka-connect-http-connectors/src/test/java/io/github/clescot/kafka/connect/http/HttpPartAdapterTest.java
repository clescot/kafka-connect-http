package io.github.clescot.kafka.connect.http;

import io.github.clescot.core.http.HttpPart;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.clescot.core.http.HttpPart.BodyType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

class HttpPartAdapterTest {
    @Nested
    class TestConstructor {
        @Test
        void test_constructor_with_struct_and_body_as_string() {
            Map<String, List<String>> headers = new HashMap<>();
            headers.put("Content-Type", List.of("application/json"));
            Struct struct = new Struct(HttpPartAdapter.SCHEMA);
            struct.put("headers", headers);
            struct.put("bodyType", STRING.toString());
            struct.put("bodyAsString", "dummy string");
            HttpPart httpPart = HttpPartAdapter.from(struct).toHttpPart();
            assertThat(httpPart.getBodyType()).isEqualTo(STRING);
            assertThat(httpPart.getContentAsString()).isEqualTo("dummy string");
        }
    }

    @Nested
    class TestToStruct{
        @Test
        void test_to_struct_content_as_byte_array() {
            HttpPart httpPart = new HttpPart("test".getBytes(StandardCharsets.UTF_8));
            Struct struct = HttpPartAdapter.from(httpPart).toStruct();
            assertThat(struct.getString("bodyType")).isEqualTo(HttpPart.BodyType.BYTE_ARRAY.toString());
            assertThat(new String(Base64.getDecoder().decode(struct.getString("bodyAsByteArray")))).isEqualTo("test");
        }

        @Test
        void test_to_struct_content_as_string() {
            HttpPart httpPart = new HttpPart("test");
            Struct struct = HttpPartAdapter.from(httpPart).toStruct();
            assertThat(struct.getString("bodyType")).isEqualTo(HttpPart.BodyType.STRING.toString());
            assertThat(struct.getString("bodyAsString")).isEqualTo("test");
        }
    }
}