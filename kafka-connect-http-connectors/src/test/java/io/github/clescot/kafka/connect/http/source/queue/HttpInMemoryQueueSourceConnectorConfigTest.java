package io.github.clescot.kafka.connect.http.source.queue;

import static io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConfigDefinition.ERROR_TOPIC;
import static io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConfigDefinition.SUCCESS_TOPIC;

import com.google.common.collect.Maps;
import java.util.HashMap;
import org.apache.kafka.common.config.ConfigException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HttpInMemoryQueueSourceConnectorConfigTest {

  @Test
  void test_null_map() {
    Assertions.assertThrows(NullPointerException.class, () -> new HttpSourceConnectorConfig(null));
  }

  @Test
  void test_empty_map() {
    HashMap<@Nullable Object, @Nullable Object> emptySettings = Maps.newHashMap();
    Assertions.assertThrows(
        ConfigException.class, () -> new HttpSourceConnectorConfig(emptySettings));
  }

  @Test
  void test_nominal_case() {
    HashMap<Object, Object> config = Maps.newHashMap();
    config.put(SUCCESS_TOPIC, "success.topic");
    config.put(ERROR_TOPIC, "error.topic");
    Assertions.assertDoesNotThrow(() -> new HttpSourceConnectorConfig(config));
  }

  @Test
  void test_missing_ack_topic() {
    HashMap<Object, Object> config = Maps.newHashMap();
    Assertions.assertThrows(ConfigException.class, () -> new HttpSourceConnectorConfig(config));
  }
}
