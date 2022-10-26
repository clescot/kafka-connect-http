#!/usr/bin/env bash

echo "Healthcheck : Start script."
connectors=$(curl -sL --noproxy localhost -H 'Content-Type: application/json' http://localhost:8083/connectors)
connectors=$(echo $connectors|jq 'join(",")' )
connectors=${connectors//,/$'\n'}
EXIT_STATUS=0

for connector in $connectors
do
    connector=${connector//\"};
    echo "Healthcheck : Check connector [$connector] tasks."

    status=$(curl  -sL --noproxy localhost -H 'Content-Type: application/json' http://localhost:8083/connectors/$connector/status)
    tasks_number=$(echo $status|jq  '.tasks|length')
    for i in $(seq 0 $((tasks_number-1))); do
        state=$(echo $status |jq ".tasks[$i].state")
        if [ "${state//\"}" == "FAILED" ]; then
            echo "Healthcheck : Connector [$connector] task [$i] is in FAILED state : restarting the task ..."
            curl -sL -X POST --noproxy localhost "localhost:8083/connectors/$connector/tasks/$i/restart"
            EXIT_STATUS=1
        fi
    done

    connector_state=$(echo $status|jq '.connector.state')
    if [ "${connector_state//\"}" == "FAILED" ]; then
        echo "Healthcheck : Connector [$connector] is in FAILED state :  restarting the connector ..."
        curl -sL -X POST --noproxy localhost localhost:8083/connectors/$connector/restart
        EXIT_STATUS=1
    fi
done

echo "Healthcheck : End script."

exit $EXIT_STATUS