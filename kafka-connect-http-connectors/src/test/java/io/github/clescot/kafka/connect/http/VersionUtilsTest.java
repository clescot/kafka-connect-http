package io.github.clescot.kafka.connect.http;

import org.junit.jupiter.api.Test;

import static io.github.clescot.kafka.connect.http.core.HttpRequest.VERSION;
import static org.assertj.core.api.Assertions.assertThat;

class VersionUtilsTest {

    @Test
    public void test_get_version(){
        assertThat(VERSION)
                .isNotNull()
                .isNotEqualTo("0.0.0");
    }

}