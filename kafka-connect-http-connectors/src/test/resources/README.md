#docker compose


## build and start the stack

`docker compose up -d`

## stop and remove the stack

`docker compose down`

## list kafka topics 


`docker run --net=host -ti confluentinc/cp-kafkacat:7.1.8  kafkacat -b $(hostname):39092 -L|grep -v leader`



