package io.github.clescot.kafka.connect.http.sink.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KafkaProducerTest {

  @Nested
  class Configure {

    @Test
    void test_null_delegate_creates_the_producer() {
      // given the production wiring, a wrapper injected without a delegate
      KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>();
      Map<String, Object> settings = Maps.newHashMap();
      settings.put("bootstrap.servers", "localhost:1");
      // when / then the NPE this used to throw is gone
      assertThatCode(
              () ->
                  kafkaProducer.configure(
                      settings, new StringSerializer(), new StringSerializer()))
          .doesNotThrowAnyException();
      kafkaProducer.close();
    }

    @Test
    void test_mock_delegate_is_kept() {
      // given the test wiring, a wrapper around a MockProducer
      Node node = new Node(1, "localhost", 9092);
      PartitionInfo partitionInfo =
          new PartitionInfo("topic", 0, node, new Node[] {}, new Node[] {});
      Cluster cluster = mock(Cluster.class);
      when(cluster.partitionsForTopic("topic")).thenReturn(Lists.newArrayList(partitionInfo));
      MockProducer<String, String> mockProducer =
          new MockProducer<>(
              cluster, true, new RoundRobinPartitioner(), new StringSerializer(), null);
      KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(mockProducer);
      Map<String, Object> settings = Maps.newHashMap();
      settings.put("bootstrap.servers", "localhost:9092");
      // when
      kafkaProducer.configure(settings, new StringSerializer(), new StringSerializer());
      // then the mock delegate is still the one in use
      assertThat(kafkaProducer.partitionsFor("topic")).containsExactly(partitionInfo);
    }
  }
}
