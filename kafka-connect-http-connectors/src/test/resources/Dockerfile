FROM confluentinc/cp-kafka-connect:7.4.1
USER root

ENV jmx_exporter_version=0.19.0
ENV kafka_connect_http_version=0.7.6

RUN curl -s -L https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${jmx_exporter_version}/jmx_prometheus_javaagent-${jmx_exporter_version}.jar -o /tmp/jmx_prometheus_javaagent.jar;
RUN curl -s -L https://github.com/clescot/kafka-connect-http/releases/download/${kafka_connect_http_version}/clescot-kafka-connect-http-connectors-${kafka_connect_http_version}.zip -o /tmp/kafka-connect-http.zip;

#KAFKA_HEAP_OPTS
#JMX_PORT
#KAFKA_JMX_OPTS

RUN confluent-hub install /tmp/kafka-connect-http.zip --no-prompt
USER appuser