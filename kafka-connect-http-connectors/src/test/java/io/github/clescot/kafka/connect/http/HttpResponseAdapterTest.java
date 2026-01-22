package io.github.clescot.kafka.connect.http;

import io.github.clescot.core.http.HttpResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;


class HttpResponseAdapterTest {
    @Nested
    class TestToStruct{
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
            assertThat(struct.getString(HttpResponse.BODY_AS_BYTE_ARRAY_FIELD)).isEqualTo(Base64.getEncoder().encodeToString("Hello World".getBytes(StandardCharsets.UTF_8)));
        }
    }

}