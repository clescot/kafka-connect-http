# install Connector into your Kafka Connect Cluster

Kafka Connect cluster is usually deployed as a Docker Container, in Kubernetes.
Note that the jar file owning these connector classes,
[need to be installed with the Kafka connect runtime](https://docs.confluent.io/kafka-connectors/self-managed/install.html#install-connector-manually).

We publish on each release, a Confluent Hub archive file (zip), useful with the confluent-hub CLI. It is not yet published on the Confluent Hub, but can be installed manually into your Docker image.

In your Dockerfile, the easiest way to install the connectors (owning the Sink and Source code) archive is to :

- copy from the github website (from the home page, in the package section, the zip file from the latest release), or
 from the the maven central repository (https://repo1.maven.org/maven2/io/github/clescot/kafka-connect-http-connectors/0.6.2/kafka-connect-http-connectors-0.6.2.zip for example)
(we've not yet published on the confluent hub website our connector).
For example, `clescot-kafka-connect-http-connectors-0.6.2.zip`.
- include it in your Kafka connect docker image, via this command in your `Dockerfile` : 
   
     `COPY connectors/clescot-kafka-connect-http-connectors*.zip /tmp/kafka-connect-http-connectors.zip`

- install the connector via the confluent-hub command line (we assume that your image is inherited from the confluent/kafka-connect image,
  which ships this useful tool), in the offline way : 
   
  `RUN confluent-hub install /tmp/kafka-connect-http.zip --no-prompt`
- you're done ! 

[An example of Dockerfile automating this  process is present in the git repository](../kafka-connect-http-connectors/src/main/resources/Dockerfile)

# check the connector installation

- you can check the installation, when you run your container, by listing the connectors installed via [the plugin REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html#connector-plugins) :
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
                 "class": "sink.io.github.clescot.kafka.connect.http.HttpSinkConnector"
             },
             {
                 "class": "source.io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector"
             },
             {
                 "class": "source.io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector"
             }
         ]
     
         
     ```

