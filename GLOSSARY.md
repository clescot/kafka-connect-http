# kafka-connect-http

A Kafka Connect plugin for HTTP. Sink connectors turn Kafka records into HTTP requests, and source connectors turn
HTTP interactions back into Kafka records.

## Language

**Publish Mode**:
What the HTTP sink connector does with each HTTP exchange's result. `NONE` discards it and `PRODUCER` publishes it to
Kafka topics.
_Avoid_: publish strategy, output mode

**Producer Mode**:
The `PRODUCER` publish mode. It writes exchanges to the Kafka topics `producer.success.topic` and
`producer.error.topic` through a low-level producer.
_Avoid_: direct mode

**In-Memory Queue**:
_(Retired)_ A JVM-local unbounded buffer that handed exchanges from an HTTP sink connector to a colocated HTTP source
connector. It was removed because it required both connectors in one worker.
_Avoid_: "in-memory mode". Do not confuse this with the OkHttp in-memory cache, which is unrelated.
