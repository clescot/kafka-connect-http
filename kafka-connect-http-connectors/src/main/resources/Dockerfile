FROM confluentinc/cp-kafka-connect:7.5.2
USER root

ENV jmx_exporter_version=0.19.0
ENV CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN="[%d] %p %X{connector.name}|%X{connector.scope}%X{connector.task} %m (%c:%L)%n"
RUN curl -s -L https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${jmx_exporter_version}/jmx_prometheus_javaagent-${jmx_exporter_version}.jar -o /tmp/jmx_prometheus_javaagent.jar;
COPY components/packages/clescot-kafka-connect-http-connectors-${project.version}.zip /tmp/kafka-connect-http.zip
RUN chmod 666 /tmp/kafka-connect-http.zip
RUN ls -al /tmp/kafka-connect-http.zip
RUN confluent-hub install /tmp/kafka-connect-http.zip --no-prompt
COPY kafka-connect.yml /tmp/kafka-connect.yml
USER appuser
