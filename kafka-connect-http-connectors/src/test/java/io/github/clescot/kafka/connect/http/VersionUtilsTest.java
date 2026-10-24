package io.github.clescot.kafka.connect.http;

import static io.github.clescot.core.http.HttpRequest.VERSION;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class VersionUtilsTest {

  @Test
  public void test_get_version() {
    assertThat(VERSION).isNotNull().isNotEqualTo("0.0.0");
  }
}
