FROM confluentinc/cp-kafka-connect:7.2.2

#Clean Kafka installation

RUN ["rm","-rf","/usr/share/doc"]
RUN ["rm","-rf","/etc/kafka-connect-hdfs"]
RUN ["rm","-rf","/etc/kafka-connect-elasticsearch"]
RUN ["rm","-rf","/etc/kafka-connect-jdbc"]
RUN ["rm","-rf","/etc/kafka-connect-s3"]

RUN ["rm","-rf","/usr/share/java/kafka-connect-hdfs"]
RUN ["rm","-rf","/usr/share/java/kafka-connect-elasticsearch"]
RUN ["rm","-rf","/usr/share/java/kafka-connect-jdbc"]
RUN ["rm","-rf","/usr/share/java/kafka-connect-s3"]


#create user
RUN groupadd -r myuser && useradd -r -g myuser myuser

RUN chgrp -R 0 /etc/confluent
RUN chmod -R g+rwX /etc/confluent

#Set timezone
LABEL "mode"="distributed"
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER myuser

VOLUME ["/data"]
EXPOSE 8083

COPY connect/healthcheck.sh /etc/confluent/docker/healthcheck.sh

#Copy extra connectors
ADD jars/kafka-connect-http-sink-ws.jar  /etc/kafka-connect/jars/kafka-connect-http-sink-ws.jar