# Architecture

![Architecture](architecture.png)

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