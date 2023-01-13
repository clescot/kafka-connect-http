# install Connector into your Kafka Connect Cluster

Kafka Connect cluster is usually deployed as a Docker Container, in Kubernetes.
Note that the jar file owning these connector classes,
[need to be installed with the Kafka connect runtime](https://docs.confluent.io/kafka-connectors/self-managed/install.html#install-connector-manually).
We publish on each release, an archive file (zip) for use on Confluent Hub. It is not yet published on the Confluent Hub, but can be installed manually into your

There are two ways to install the connectors :  
 - the `confluentHub archive` way

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

- the `uberjar` way
  - copy the `uberjar` published in the `/usr/local/share/kafka/plugins` directory, in your Dockerfile, from the [confluentinc/cp-kafka-connect Docker image](https://hub.docker.com/r/confluentinc/cp-kafka-connect).
  - the main difficulty is to find the right place to put the jar, depending on your kafka-connect image. the `confluentHub archive` way permits to avoid this issue

# check the connector installation

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

