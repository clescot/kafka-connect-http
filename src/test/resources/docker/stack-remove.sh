#!/usr/bin/env bash
echo "/!\ REMOVE sink ws";

my_ip=$(ip route get 8.8.8.8 | awk '/8.8.8.8/ {print $NF}')
export DOCKER_HOST_ADDRESS=${DOCKER_HOST_ADDRESS:-$my_ip}

curl -X DELETE "http://$DOCKER_HOST_ADDRESS:8083/connectors/http-sink-ws"
docker -H tcp://${DOCKER_HOST_ADDRESS}:2375 stack rm sink-ws;

echo "sink ws connect is removed";
