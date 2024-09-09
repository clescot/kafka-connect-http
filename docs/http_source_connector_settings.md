# HTTP Source Connector

The HTTP Source connector is only useful if you have configured the publishment of HTTP interactions into the queue,
via the `publish.to.in.memory.queue` set to `true`.
There is no need to configure an HttpSource connector if your publish mode in Http Sink Connector is 'NONE' or 'PRODUCER'.
Only the 'IN_MEMORY_QUEUE' publish mode require to setup an HTTP Source connector.

#### required HTTP Source connector parameters

- *`success.topic`* : Topic to receive successful http request/responses, for example, http-success
- *`error.topic`* : Topic to receive errors from http request/responses, for example, http-error

#### optional HTTP Source connector parameters

- *`queue.name`* : if not set, listen on the 'default' queue.

#### Configuration example


`source.json` example :
```json 
{
    "name": "my-http-source-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.source.HttpSourceConnector",
    "tasks.max": "1",
    "success.topic": "http-success",
    "error.topic": "http-error"
    }
}
```

You can create or update this connector instance with this command :

```bash
curl -X PUT -H "Content-Type: application/json" --data @source.json http://my-kafka-connect-cluster:8083/connectors/my-http-source-connector/config
```

3. Http Sink and Source Connectors

> :warning: Below are some characteristics only applicable when the publish mode is set to `IN_MEMORY_QUEUE`.

#### are linked

HTTP Sink connector can be used without the HTTP Source connector, if the `publish` mode is not set to `IN_MEMORY_QUEUE`.

HTTP Source Connector cannot be used without the HTTP Sink connector.

#### instantiated in the same location

When Http Sink and Source connectors are configured together, they must be **instantiated in the same place**, to exchange data through the in memory queue.
The above configuration permits to fullfill these needs.

#### in the same classloader

Kafka Connect, loads connector plugins in isolation : each zip owning a plugin and its dependencies,
are loaded with a dedicated classloader, to avoid any dependencies conflicts.

To avoid any isolation issue between the Http Sink and Source plugin (to permit to exchange data via the in memory queue),
and to ease the install process, we ship them in the same jar (contained in the same zip archive).
So, any Http **`Sink`** connector, will have the ability to exchange data with the **Source** connector in the same classloader.
On this field, you have no actions to do.

#### on the same kafka connect instance

To exchange data, also if these Connector classes are located in the same jar, they need to be instantiated both on the same kafka connect instance,
with the configuration explained below.

#### same partitioning for topics used by HTTP sink and source connectors.

To divide the work to be done, Kafka provide **partitioning** :
if data can be handled independently (without any requirement on order), you can split data between partitions to level up your throughput **if needed**.

So, with multiple partitions, there are multiple parts of data to handle : **you must configure the same number of partitions** for topics used by the HTTP Sink and Source connectors,
and don't change the partition algorithm.
So, a record with a key in the topic used by the sink connector, will result in an HTTP Exchange,
which will be stored in the same partition in the topic used by the Source connector (if you don't choose a round-robin algorithm),
because the in memory queue preserves the key linked to the record.


#### same tasks.max parameter

you have to configure a *`tasks.max`* parameter for each connector : the kafka connect cluster will distribute partitions among tasks.
Each connector instance will handle up to the `tasks.max` configuration parameter.
Kafka connect distributes tasks among workers, which are processes that execute connectors and tasks.

To avoid any issue, you must configure the same `tasks.max` parameters for the HTTP Sink and Source connectors.
