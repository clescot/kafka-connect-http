# install Connector into your Kafka Connect Cluster

Kafka Connect cluster is usually deployed as a Docker Container, in Kubernetes.

In your Dockerfile, the easiest way to install the connectors (owning the Sink and Source code) archive is to :

- copy from the github website, from the home page, in the package section, the zip file from the latest release 
(we've not yet published on the confluent hub website our connector, nor on the maven central repository).
For example, `kafka-connect-http-sink-0.2.32.zip`.
- include it in your Kafka connect docker image, via this command in your `Dockerfile` : 
`COPY connectors/kafka-connect-http-sink-*.zip /tmp/kafka-connect-http-sink.zip`
- install the connector via the confluent-hub command line (we assume that your image is inherited from the confluent/kafka-connect image,
which ships this useful tool), in the offline way : 
`RUN confluent-hub install /tmp/kafka-connect-http-sink.zip --no-prompt`
- you're done ! 
- you can check the installation, when you run your container, by listing the connectors installed via (the plugin REST API)[https://docs.confluent.io/platform/current/connect/references/restapi.html#connector-plugins] :
```
GET /connector-plugins/ HTTP/1.1
Host: connect.example.com
```

and see in the response, among other plugins : 

```
    Example response:

    HTTP/1.1 200 OK

    [
        {
            "class": "com.github.clescot.kafka.connect.http.sink.HttpSinkConnector"
        },
        {
            "class": "com.github.clescot.kafka.connect.http.source.HttpSourceConnector"
        }
    ]

    
```