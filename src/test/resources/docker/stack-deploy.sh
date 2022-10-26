#!/usr/bin/env bash
start=`date +%s`;
timeout=300
my_ip=$(ip route get 8.8.8.8 | awk '/8.8.8.8/ {print $NF}')
export DOCKER_HOST_ADDRESS=${DOCKER_HOST_ADDRESS:-$my_ip}
echo "DOCKER_HOST_ADDRESS=${DOCKER_HOST_ADDRESS}";

echo "$(docker -H tcp://${DOCKER_HOST_ADDRESS}:2375 -v)"

#initialize swarm if not already done
swarm_installed="$(docker -H tcp://${DOCKER_HOST_ADDRESS}:2375 info |grep 'Swarm: active')"
echo "swarm_installed='$swarm_installed'"
if [[ -z "${swarm_installed// }" ]]; then
    docker -H tcp://${DOCKER_HOST_ADDRESS}:2375 swarm init
    echo "Docker Swarm initialized";
else
    echo "Docker Swarm already initialized";
fi


echo "initialize kafka connect http sink stack";




DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";

function deploy_stack {
    echo "deploying stack $1";
    echo "with command: docker -H tcp://${DOCKER_HOST_ADDRESS}:2375 stack deploy --compose-file=\"$DIR/$1.yml\" --prune --with-registry-auth $1"
    docker -H tcp://${DOCKER_HOST_ADDRESS}:2375 stack deploy --compose-file="$DIR/$1.yml" --prune --with-registry-auth "$1";
    echo "waiting $2 seconds for $1 stack to be up";
    end=`date +%s`
    runtime=$((end-start))
    echo "$runtime seconds since start : timeout $timeout seconds"
    if [ "$runtime" -gt "$timeout" ]; then
        echo "timeout $timeout reached! => exit 1";
        exit 1;
    fi
    sleep $2;

}
export $(cat "ws-dev.env" | grep -v ^#)
deploy_stack sink-ws 30;

connectConfig="{
       \"name\":\"http-sink\",
       \"config\":{
          \"connector.class\":\"com.github.clescot.kafka.connect.http.sink.WsSinkConnector\",
          \"tasks.max\":\"1\",
          \"group.id\":\"http-sink\",
          \"topics\":\"http-sink-urls\",
          \"connect.sink.target.bootstrap.server\":\"$KAFKA_BOOTSTRAP_SERVERS\",
          \"connect.sink.target.schema.registry\":\"$SCHEMA_REGISTRY_URL\",
          \"connect.sink.ack.topic\":\"http_sink_response__v1\",
          \"connect.sink.ack.schema\":\"{\\\"type\\\": \\\"record\\\", \\\"name\\\": \\\"g2r\\\", \\\"fields\\\": [ { \\\"name\\\": \\\"correlationId\\\", \\\"type\\\": \\\"string\\\" }, { \\\"name\\\": \\\"responseCode\\\", \\\"type\\\": \\\"int\\\" }, { \\\"name\\\": \\\"content\\\", \\\"type\\\": \\\"string\\\" } ] }\"
       }
    }"
connectUrl="http://$DOCKER_HOST_ADDRESS:8083/connectors"

curl -X DELETE "$connectUrl/sink-ws-$USER_NAME"
curl -X POST -H  "Content-Type: application/json" --data "$connectConfig" "$connectUrl"

docker -H tcp://${DOCKER_HOST_ADDRESS}:2375 service logs http-sink-connector

echo "all stacks are deployed";
echo "happy Docker Swarm Mode riding :-)";
exit 0
