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

# Http Sink Connector

![Http Sink Connector organization](http_sink_connector.png)

1. The HTTP Sink connector listen to the incoming HttpRequest topics (can be a list of topics, or a regex, via *topics* or *topics.regex* settings)
2. a converter convert the byte array either as a string, or from a binary format described in a schema (Avro, JSON Schema or Protobuf)  to a Struct
3. a sink task which get multiple records each time, convert the record (String or Struct) into an HttpRequest object.
4. the sink task select the right configuration according to the predicate bound to it (the HttpRequest is tested against the predicate).
5. if configured, the configuration add some additional static headers.
6. if configured, the configuration add a correlation id if not found.
7. if configured, the configuration add a request id if not found.
8. a request is asked to the HTTP client
9. a throttling via the rate limiter is applied
10. the request is executed against the web site
11. the response is received, and the HttpExchange is built and enriched with a status (based on the `config.<idconfig>.enrich.exchange.success.response.code.regex`)
12. a decision is made against the HttpExchange status build in the previous stage
13. if the HttpExchange status is a success, the HttpExchange is serialized into the in memory queue
14. if the HttpExchange status is not a success, an evaluation is done to know if the error is retryable (based on the `config.<idconfig>.retry.policy.response.code.regex`)
15. if the error is not retryable, or the retry attempts limit is reched, the failing HttpExchange is serialized into the in memory queue.
16. if the error is retryable, another HTTP call is done.