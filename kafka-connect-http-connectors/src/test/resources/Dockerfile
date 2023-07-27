FROM confluentinc/cp-kafka-connect:7.4.1
USER root

COPY target/components/packages/clescot-kafka-connect-http-connectors-*.zip  /tmp/kafka-connect-http.zip
## to customize the JVM running Kafka Connect
##ENV KAFKA_HEAP_OPTS
RUN confluent-hub install /tmp/kafka-connect-http.zip --no-prompt
USER appuser