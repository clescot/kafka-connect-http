package io.github.clescot.kafka.connect.http.jexl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.Year;
import java.time.ZoneOffset;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.MapContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JexlEngineFactoryTest {

  private JexlEngine jexlEngine;

  @BeforeEach
  void setUp() {
    jexlEngine = JexlEngineFactory.build();
  }

  @Nested
  class DateTimeNamespace {

    @Test
    void test_now_is_available() {
      Object now = jexlEngine.createExpression("datetime:now()").evaluate(new MapContext());

      assertThat(now).isInstanceOf(String.class);
      assertThatCode(() -> Instant.parse((String) now)).doesNotThrowAnyException();
    }

    @Test
    void test_now_accepts_a_pattern() {
      int yearBefore = Year.now(ZoneOffset.UTC).getValue();
      Object year = jexlEngine.createExpression("datetime:now('yyyy')").evaluate(new MapContext());
      int yearAfter = Year.now(ZoneOffset.UTC).getValue();

      assertThat(year).isIn(String.valueOf(yearBefore), String.valueOf(yearAfter));
    }

    @Test
    void test_format_rejects_an_epoch_millis_value() {
      MapContext context = new MapContext();
      context.set("timestampInMs", 1699228800000L);

      assertThatThrownBy(
              () ->
                  jexlEngine
                      .createExpression("datetime:format(timestampInMs,'yyyy')")
                      .evaluate(context))
          .isInstanceOf(RuntimeException.class);
    }
  }

  @Nested
  class Sandbox {

    @Test
    void test_a_class_outside_the_permissions_cannot_be_instantiated() {
      assertThatThrownBy(
              () ->
                  jexlEngine
                      .createExpression("new('java.io.File','/tmp/x')")
                      .evaluate(new MapContext()))
          .isInstanceOf(RuntimeException.class);
    }
  }
}
