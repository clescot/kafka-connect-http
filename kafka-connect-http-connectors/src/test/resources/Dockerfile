FROM confluentinc/cp-kafka-connect:7.4.1
USER root

ENV jmx_exporter_version=0.19.0
ENV kafka_connect_http_version=0.7.6

RUN curl -s -L https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${jmx_exporter_version}/jmx_prometheus_javaagent-${jmx_exporter_version}.jar -o /tmp/jmx_prometheus_javaagent.jar;
ADD ./components/packages/clescot-kafka-connect-http-connectors-*.zip /tmp/kafka-connect-http.zip
RUN chmod 666 /tmp/kafka-connect-http.zip
RUN ls -al /tmp/kafka-connect-http.zip
#RUN curl -s -L https://github.com/clescot/kafka-connect-http/releases/download/${kafka_connect_http_version}/clescot-kafka-connect-http-connectors-${kafka_connect_http_version}.zip -o /tmp/kafka-connect-http.zip;
RUN confluent-hub install /tmp/kafka-connect-http.zip --no-prompt
COPY ./test-classes/kafka-connect.yml /tmp/kafka-connect.yml
USER appuser
