#!/usr/bin/env bash

cd ..
mvn clean package -DskipTests
export CLASSPATH="$(find target/ -type f -name '*.jar' | tr '\n' ':')"
export KAFKA_JMX_OPTS="-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=${SUSPEND},address=5005"
docker run -v ${pwd}:/config confluentinc/cp-kafka-connect:5.3.1   connect-standalone   /config/connect-avro-docker.properties /config/sink-connector.properties