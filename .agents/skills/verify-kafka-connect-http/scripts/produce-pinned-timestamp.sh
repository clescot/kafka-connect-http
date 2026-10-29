#!/usr/bin/env bash
#
# Produce one String value with an explicit Kafka record timestamp.
#
#   produce-pinned-timestamp.sh <bootstrap> <topic> <epoch-millis> <value>
#
# The sink JEXL recipe needs this: timestamp/timestampInMs/dateTime are read
# from the record timestamp, so only a pinned timestamp proves the expressions
# used the record's event time and not the wall clock. The Kafka console
# producer cannot set a record timestamp.
#
set -euo pipefail

[ "$#" -eq 4 ] || {
  echo "usage: $0 <bootstrap> <topic> <epoch-millis> <value>" >&2
  exit 2
}

BOOTSTRAP="$1"
TOPIC="$2"
EPOCH_MILLIS="$3"
VALUE="$4"

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(git -C "$SKILL_DIR" rev-parse --show-toplevel)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pinned-producer.XXXXXX")"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

cat > "$WORK/logback.xml" <<'XML'
<configuration>
  <root level="WARN"/>
</configuration>
XML

cat > "$WORK/PinnedTimestampProducer.java" <<'JAVA'
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public final class PinnedTimestampProducer {
  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
    try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
      RecordMetadata metadata =
          producer
              .send(new ProducerRecord<>(args[1], null, Long.parseLong(args[2]), null, args[3]))
              .get();
      System.out.printf(
          "produced %s-%d@%d%n", metadata.topic(), metadata.partition(), metadata.timestamp());
    }
  }
}
JAVA

mvn -q -f "$REPO_ROOT/pom.xml" -pl kafka-connect-http-connectors dependency:build-classpath \
  -Dmdep.outputFile="$WORK/classpath.txt" >/dev/null

java -cp "$WORK:$(cat "$WORK/classpath.txt")" "$WORK/PinnedTimestampProducer.java" \
  "$BOOTSTRAP" "$TOPIC" "$EPOCH_MILLIS" "$VALUE"
