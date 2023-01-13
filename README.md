# Kafka Connect HTTP Sink and (optionally) Source connectors project

## 1. project intention

### What is the goal of this project?

The main goal of this project is to allow to interact with HTTP servers, via a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
cluster.
It permits to define HTTP requests, and store optionally HTTP responses as Kafka messages, with Kafka connect connectors.

### Advantages of this project

It permits to query finely HTTP servers and store their responses with the High level Kafka Connect configuration.

Other Kafka Connect Connectors permits to interact with HTTP servers.

But they have got some restrictions :

- some projects ignore HTTP responses
- some projects which don't ignore HTTP responses, mix a high level Kafka Connect configuration to query, with a low level configuration to store HTTP responses. 
- some projects have got a proprietary licence


### What is the problem with Kafka Connect and HTTP ? 

Kafka connect permits to copy data from/to a Kafka Cluster easily, but not to interact (request/response) with an external ressource.

Kafka Connect divide tasks between Sink and
Source connectors :

- **Sink** connectors get data from Kafka and output to an external data target
- **Source** Connectors get data from an external data source and output it to Kafka 

The main advantage of this separation is to ease data interactions.

The main problem with HTTP interactions, is its request/response nature.

#### How does it NOT work ?

- *the One connector strategy ?*

    If you define only one connector, you can read a kafka message (with the reusable high level Kafka connect configuration), and query an HTTP server. If you want to store HTTP responses, 
    you need to define a low level kafka client which will translate HTTP responses as kafka messages : you duplicate the Kafka configuration, with some low level and high level configurations mixed.
    Your configuration can became complex...
    This strategy can work if you don't bother with HTTP responses (but who don't ?), and don't configure a low level kafka client.

- *Multiple connectors to solve the problem ?*
    
    Sink and Source connectors share interactions with the Kafka Cluster. You can easily, out of the box, define a *Source* Connector 
  which will listen to an external Datasource, and store data into Kafka. You can also define a *Sink* Connector which 
  will listen to Kafka, and output data to a target. But  it reverses HTTP interactions ; we don't receive HTTP reponses 
  before querying an HTTP server ; we cannot declare a *Source* Connector, which will chain through Kafka with a *Sink* Connector : 
- this is not a solution to our problem.

### How does it work? How do we solve the problem ?
    
We need to revert the multiple connectors proposal in the previous section, with a shared channel different from Kafka. We provide :

- a **Sink** Connector to query HTTP servers

    We define a Sink Connector to read from kafka, and query HTTP servers according to the Kafka message. This is the most easy part.
- an optional **Source** Connector to store HTTP responses
 
  If we need to store HTTP responses, the **Sink** Connector need to publish to the **Source** Connector the responses. 
  We use **an internal unbounded in memory Thread-safe Queue** for this purpose. Global HTTP interactions are (request,responses,
  and metadatas) published in the in memory queue. Note that the Source connector is optional (you don have to configure an in memory queue in this case), 
  and if you configure multiple connectors pairs (Sink and Source), you can define a unique in memory queue (with the `queue.name` parameter) for each pair.

### Does it cancel the distributed nature of Kafka Connect ?

No, you can distribute http queries between multiple Kafka Connect instances. The local nature is only for the correlation between
HTTP query and HTTP responses.

### Does the unbounded in memory queue implies an *OutOfMemoryError* risk ?

As both ends of the in memory queue, implies a Kafka communication, the *OutOfMemory* risk seems mitigated by the same source of problem on both sides (kafka communication problem).
We also check that all queues registered has got their consumer (Source Connector instance).
Note that a queue has got only one consumer, opposite to the Topic concept, which support multiple consumers. The only one queue consumer, is the configured Source Connector.

# 2. Architecture

![Architecture](docs/architecture.png)

This big picture permits to highlight (with numbers in the picture) some key points in the architecture, which will be explained with details
after the section dedicated to put in place the HTTP Sink and Source connectors:

1. format of the [incoming kafka message](docs/incoming_message_format.md) (HTTP intention)
2. The HTTP Sink connector listen to these topics (can be a list of topics, or a regex, via *topics* or *topics.regex* settings)
3. The HTTP Sink connector transform the incoming message into an HTTP call and get the answer from the HTTP server.
  Behaviour of the HTTP client shipped with the HTTP Sink Connector can be tuned, including the retry policy. At this stage,
  the HTTP Sink connector, according to the HTTP exchange details, decides if the HTTP exchange is a success or a failure.
  Incoming message can be a JSON serialized as a String, or a structured message linked with a schma stored in a schema registry. 
4. Optionally (`publish.to.in.memory.queue` set to `true` in the HTTP Sink Connector configuration), the HTTP exchange 
   is published into a `default` in memory queue (or a defined queue with the `queue.name` parameter in the HTTP Sink  
   Connector configuration). A check is done to prevent publishment to a 'in memory' queue without consumer(Source Connector),  
   i.e preventing an OutofMemory Error. The HTTP Sink Connector will fail at the first message consumption in this situation.   
5. If configured in the Sink Configuration, an HTTP Source Connector is needed to consume the published 
   HTTP exchange (with all the details of the interaction).
6. According to the HTTP Exchange status (success or failure), the HTTP Source connector serialize the exchange as a kafka message and save
   it in the `http.success` topic, or the `http.error` topic.
7. the HTTP exchange is saved as a `Struct`, and can be serialized as a JSON Schema or another format via converters, and the help of a Schema registry.
   It also can be serialized as a String. 

# 2. [how to install](docs/install.md)
# 3. [HTTP Connectors configuration](docs/connectors_configuration.md)
# 4. [incoming message format](docs/incoming_message_format.md)
# 5. [request handling](docs/request_handling.md)
# 6. [outcoming message format](docs/outcoming_message_format.md)
# 7. [missing features](docs/missing_features.md)



