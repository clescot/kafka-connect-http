package io.github.clescot.kafka.connect.http.sink.publish;

public enum PublishMode {
  /**
   * with a Sink Connector, grab HTTP messages to query from a topic, and publish HTTP results in a
   * final topic with an embedded low level producer
   */
  PRODUCER,
  /**
   * with a Sink Connector, grab HTTP messages to query from a topic, and does NOT publish HTTP
   * results
   */
  NONE
}
