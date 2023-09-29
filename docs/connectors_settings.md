# HTTP Connectors settings

Here are the settings to setup an HTTP Sink connector, and optionally an HTTP Source Connector, into a Kafka Connect
cluster. 

### HTTP Sink Connector

#### required parameters

every Kafka Connect Sink Connector need to define these required parameters :

- *`connector.class`* : `io.github.clescot.kafka.connect.http.sink.HttpSinkConnector`
- *`topics`* (or *`topics.regex`*): `http-requests` for example

#### optional Kafka Connect parameters

- *`tasks.max`*  (default to `1`)
- *`key.converter`*
- *`value.converter`*
- ....

#### publish into the _in memory_ queue : 

- *`publish.to.in.memory.queue`* : `false` by default. When set to `true`, publish HTTP interactions (request and responses)
  are published into the in memory queue.
- *`queue.name`* : if not set, `default` queue name is used, if the `publish.to.in.memory.queue` is set to `true`.
  You can define multiple in memory queues, to permit to publish to different topics, different HTTP interactions. If
  you set this parameter to a value different than `default`, you need to configure an HTTP source Connector listening
  on the same queue name to avoid some OutOfMemoryErrors.
- *`wait.time.registration.queue.consumer.in.ms`* : wait time for a queue consumer (Source Connector) registration. default value is 60 seconds.
- *`poll.delay.registration.queue.consumer.in.ms`* : poll delay, i.e, wait time before start polling a registered consumer. default value is 2 seconds.
- *`poll.interval.registration.queue.consumer.in.ms`* : poll interval, i.e, time between every poll for a registered consumer. default value is 5000 milliseconds.

### Metrics Registry

metrics registry can be configured to add some metrics, and to export them. Metrics registry is global to the JVM.
Only _okhttp_ HTTP client support this feature.

#### add some HTTP metrics
When at least one of the export is activated, a listener is added to the okhttp to expose some metrics as timers :
- `okhttp`
- `okhttp.dns`
- `okhttp.socket.connection`
- `okhttp.pool.connection`
- `okhttp.proxy.select`
- `okhttp.request.headers`
- `okhttp.request.body`
- `okhttp.response.headers`
- `okhttp.response.body`

Each metrics with its name listed above, is bound to some tags/dimensions :
- `configuration.id`
- `host` : metrics identical to target.host, and present for old systems relying on this metric. Not activated by default. can be activate with `meter.registry.tag.include.legacy.host` set to `true`.
- `method` : http method
- `outcome` : response code or `UNKNOWN`
- `status` : response code, `IO_ERROR`, or `CLIENT_ERROR`
- `target.host` : http remote host
- `target.port` : http remote port
- `target.scheme` : http scheme used to interact with remote http server
- `target.uri` : will be `none`, except if you activate it with `meter.registry.tag.include.url.path` set to `true`. Beware of the high cardinality metrics issue if you've got many different paths in urls (https://last9.io/blog/how-to-manage-high-cardinality-metrics-in-prometheus/)


#### other metrics are available

Some built-ins metrics are available :
- executor services metrics can be activated with `meter.registry.bind.metrics.executor.service` set to `true`
- _JVM memory_ metrics can be activated with `meter.registry.bind.metrics.jvm.memory` set to `true`
- _JVM threads_ metrics can be activated with `meter.registry.bind.metrics.jvm.thread` set to `true`
- _JVM info_ metrics can be activated with `"meter.registry.bind.metrics.jvm.info"` set to `true`
- _JVM Garbage Collector_ (GC) metrics can be activated with `"meter.registry.bind.metrics.jvm.gc"` set to `true`
- _JVM classloaders_ metrics can be activated with `"meter.registry.bind.metrics.jvm.classloader"` set to `true`
- _JVM processors_ metrics can be activated with `"meter.registry.bind.metrics.jvm.processor"` set to `true`
- _Logback_ metrics can be activated with `"meter.registry.bind.metrics.logback"` set to `true`


#### export metrics
Both exports (JMX and Prometheus) can be combined.

- JMX export
  you need to activate this export with :
  `"meter.registry.exporter.jmx.activate": "true"`


- prometheus export
  you need to activate this export with :
  `"meter.registry.exporter.prometheus.activate": "true"`
  by default, the port open is the default prometheus one (`9090`), but you can define yours with this setting :
  `"meter.registry.exporter.prometheus.port":"9087`

#### configuration

The connector ships with a `default` configuration, and we can, if needed, configure more configurations.
A configuration is identified with a unique `id`.

Configurations are listed by their id, divided by comma (`,`), with the property : `config.ids`.

example : ```"config.ids":"config1,config2,config3"```.

Note that the `default` configuration is always created, and must not be referenced in the `config.ids` property.


For example, the `test4` configuration will have all its settings starting with the `config.test4` prefix,
following by other prefixes listed above. 

##### predicate
A configuration apply to some http requests based on a predicate  : all http requests not managed by a configuration are catch by the `default` configuration)
All the settings of the predicate, are starting with the `config.<configurationId>.predicate`.
The predicate permits to filter some http requests, and can be composed, cumulatively, with : 

- an _URL_ regex with the settings for the `default` configuration : ```"config.default.predicate.url.regex":"myregex"``` 
- a _method_ regex with the settings for the `default` configuration : ```"config.default.predicate.method.regex":"myregex"``` 
- a _bodytype_ regex with the settings for the `default` configuration : ```"config.default.predicate.bodytype.regex":"myregex"``` 
- a _header key_ with the settings for the `default` configuration (despite any value, when alone) : ```"config.default.predicate.header.key":"myheaderKey"```
- a _header value_ with the settings for the `default` configuration (can only be configured with a header key setting) : ```"config.default.predicate.header.value":"myheaderValue"```

- can enrich HttpRequest by 
  - adding static headers with the settings for `default` configuration : 
  ``` 
   "config.default.enrich.request.static.header.names":"header1,header2"
   "config.default.enrich.request.static.header.header1":"value1"
   "config.default.enrich.request.static.header.header2":"value2"
  ```
  
  - generating a missing correlationId  with the settings for the `default` configuration : ``` "config.default.enrich.request.generate.missing.correlation.id":true ```
  - generating a missing requestId with the settings for the `default` configuration : ``` "config.default.enrich.request.generate.missing.request.id":true ```
- can enrich the HttpExchange with a success regex
- owns a rate limiter
- owns a retry policy
- owns an HTTP Client  with the available settings : 
  - retry settings (**set them all or no one**), permit to define a default retry policy.
    - *`config.default.retry.policy.response.code.regex`* : regex which define if a retry need to be triggered, based on the response status code. default is `^[1-2][0-9][0-9]$`
      by default, we don't resend any http call with a response between `100` and `499`.only `5xx` by default, trigger a resend
      - `1xx` is for protocol information (100 continue for example),
      - `2xx` is for success,
      - `3xx` is for redirection
      - `4xx` is for a client error
      - `5xx` is for a server error
    - *`config.default.retries`* : if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how many retries before an error is thrown
    - *`config.default.retry.delay.in.ms`* : if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long wait initially before first retry
    - *`config.default.retry.max.delay.in.ms`* : if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long max wait before retry
    - *`config.default.retry.delay.factor`* : if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define the factor to multiply the previous delay to define the current retry delay
    - *`config.default.retry.jitter.in.ms`* : if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object.
  - rate limiting settings
    - *`config.default.rate.limiter.period.in.ms`* : period of time in milliseconds, during the max execution cannot be exceeded
    - *`config.default.rate.limiter.max.executions`* : max executions in the period defined with the 'httpclient.default.rate.limiter.period.in.ms' parameter
    - *`config.default.rate.limiter.scope`* : can be either `instance` (default option when not set, i.e a rate limiter per configuration in the connector instance),  or `static` (a rate limiter per configuration id shared with all connectors instances in the same Java Virtual Machine.
    - - owns a retry regex
  - header settings
    - *`config.default.static.request.header.names`* : list of headers names to attach to all requests. *Static* term, means that these headers
      are not managed by initial kafka message, but are defined at the connector level and added globally. this list is divided by
      `,` character. The connector will try to get the value to add to request by querying the config with the header name as parameter name.
      For example, if set `static.request.header.names: param_name1, param_name2`, the connector will lookup the param_name1
      and param_name2 parameters to get values to add.
    - *`config.default.generate.missing.request.id`* : `false` by default. when set to `true`, generate an uuid bound to the `X-Request-ID` header.
    - *`config.default.generate.missing.correlation.id`* : `false` by default. when set to `true`, generate an uuid bound to the `X-Correlation-ID` header.
  - http client authentication parameters
    - `basic` authentication settings
      - *`config.default.httpclient.authentication.basic.activate`* : activate `basic` authentication response with credentials, for web sites matching this configuration (_required_)
      - *`config.default.httpclient.authentication.basic.username`* : username used to authenticate against the `basic` challenge (_required_)
      - *`config.default.httpclient.authentication.basic.password`* : password used to authenticate against the `basic` challenge (_required_)
      - *`config.default.httpclient.authentication.basic.charset`* : character set used by the http client to encode `basic` credentials (_optional_ `ISO_8859_1` if not set)
    - `digest` authentication settings
      - *`config.default.httpclient.authentication.digest.activate`* : activate `digest` authentication response with credentials, for web sites matching this configuration (_required_)
      - *`config.default.httpclient.authentication.digest.username`* : username used to authenticate against the `digest` challenge (_required_)
      - *`config.default.httpclient.authentication.digest.password`* : password used to authenticate against the `digest` challenge (_required_)
      - *`config.default.httpclient.authentication.digest.charset`* : character set used by the http client to encode `digest` credentials (_optional_ `US-ASCII` if not set) 
      - *`config.default.httpclient.authentication.digest.secure.random.prng.algorithm`* : pseudo random number generation algorithm name according to the [java supported random algorithm names](https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#securerandom-number-generation-algorithms) default to `SHA1PRNG`. 
  - http client SSL parameters
    - *`config.default.httpclient.ssl.keystore.path`* : file path of the custom key store.
    - *`config.default.httpclient.ssl.keystore.password`* : password of the custom key store.
    - *`config.default.httpclient.ssl.keystore.type`* : keystore type. can be `jks` or `pkcs12`.
    - *`config.default.httpclient.ssl.keystore.algorithm`* : the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.
    - *`config.default.httpclient.ssl.truststore.path`* : file path of the custom trust store.
    - *`config.default.httpclient.ssl.truststore.password`* : password of the custom trusted store.
    - *`config.default.httpclient.ssl.truststore.type`* : truststore type. can be `jks` or `pkcs12`.
    - *`config.default.httpclient.ssl.truststore.algorithm`* : the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.
    - *`config.default.httpclient.ssl.truststore.always.trust`* : add a truststore that always trust *any* certificates. Transport security is disabled. Be careful that the server cannot be trusted with this option !
  - http client proxy settings
    - *`config.default.proxy.httpclient.hostname`* : hostname of the proxy. 
    - *`config.default.proxy.httpclient.port`* : port of the proxy.
    - *`config.default.proxy.httpclient.type`* : proxy type. can be either `HTTP` (default), `DIRECT` (i.e no proxy), or `SOCKS`.
  - http client proxy selector settings (`<index`> is an increasing integer starting with 0)
    - *`config.default.proxyselector.httpclient.algorithm`* : algorithm of the proxy selector.can be `uriregex`, `random`, `weightedrandom`, or `hosthash`. Default is `uriregex`. 
    - *`config.default.proxyselector.httpclient.<index>.hostname`* : host of the first entry proxy list.
    - *`config.default.proxyselector.httpclient.<index>.port`* : port of the proxy.
    - *`config.default.proxyselector.httpclient.<index>.type`* : proxy type. can be either `HTTP` (default), `DIRECT` (i.e no proxy), or `SOCKS`.
    - *`config.default.proxyselector.httpclient.<index>.uri.regex`* : uri regex matching this proxy ;  settings only for the `uriregex` algorithm.
    - *`config.default.proxyselector.httpclient.<index>.weight`* : integer used with the `weightedrandom` algorithm.
    - *`config.default.proxyselector.httpclient.non.proxy.hosts.uri.regex`* : hosts which don't need to be proxied to be reached.
  - http client async settings
    - *`httpclient.async.fixed.thread.pool.size`* : custom fixed thread pool size used to execute asynchronously http requests.
  - http client implementation settings (prefixed by `config.<config_id>` )
    - *`httpclient.implementation`* : define which installed library to use : either `ahc`, a.k.a async http client, or `okhttp`. default is `okhttp`.  
  - _okhttp_ (default) HTTP client implementation settings
    - *`config.default.okhttp.connection.pool.scope`*  : scope of the connection pool. can be either 'instance' (i.e a connection pool per configuration in the connector instance),  or 'static' (a connection pool shared with all connectors instances in the same Java Virtual Machine).
    - *`config.default.okhttp.connection.pool.max.idle.connections`* : amount of connections to keep idle, to avoid the connection creation time when needed. Default is 0 (no connection pool).
    - *`config.default.okhttp.connection.pool.keep.alive.duration`* : Time in milliseconds to keep the connection alive in the pool before closing it. Default is 0 (no connection pool).
    - *`config.default.okhttp.protocols`* : the protocols to use, in order of preference. If the list contains 'H2_PRIOR_KNOWLEDGE' then that must be the only protocol and HTTPS URLs will not be supported. Otherwise the list must contain 'HTTP_1_1'. The list must not contain null or 'HTTP_1_0'.
    - *`config.default.okhttp.ssl.skip.hostname.verification`* : if set to 'true', skip hostname verification. Not set by default.
    - *`config.default.okhttp.connect.timeout`* : Sets the default connect timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.
    - *`config.default.okhttp.call.timeout`* : default timeout in milliseconds for complete call . A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.
    - *`config.default.okhttp.read.timeout`* : Sets the default read timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.
    - *`config.default.okhttp.write.timeout`* : Sets the default write timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.
    - *`config.default.okhttp.follow.redirect`* : does the http client need to follow a redirect response from the server. default to true.
    - *`config.default.okhttp.follow.ssl.redirect`* : does the http client need to follow an SSL redirect response from the server. default to true.
    - *`config.default.okhttp.cache.activate`* (`true` to activate, `false` by default)
    - *`config.default.okhttp.cache.max.size`* (default `10000` max cache entries)
    - *`config.default.okhttp.cache.directory.path`* (default `/tmp/kafka-connect-http-cache` directory path for `file` type, default `/kafka-connect-http-cache` for `inmemory` type)
    - *`config.default.okhttp.cache.type`* (default `file`, and can be set to `inmemory`)
  - _Async Http Client (AHC)_ implementation settings
    - *`org.asynchttpclient.http.max.connections`* :  (default `3`)
    - *`org.asynchttpclient.http.rate.limit.per.second`* (default `3`)
    - *`org.asynchttpclient.http.max.wait.ms`* (default `500 ms`)
    - *`org.asynchttpclient.keep.alive.class`* (default `org.asynchttpclient.channel.DefaultKeepAliveStrategy`)
    - *`org.asynchttpclient.response.body.part.factory`* (default `EAGER`)
    - *`org.asynchttpclient.connection.semaphore.factory`* (default `org.asynchttpclient.netty.channel.DefaultConnectionSemaphoreFactory`)
    - *`org.asynchttpclient.cookie.store`* (default `org.asynchttpclient.cookie.ThreadSafeCookieStore`)
    - *`org.asynchttpclient.netty.timer`* (default `io.netty.util.HashedWheelTimer`)
    - *`org.asynchttpclient.byte.buffer.allocator`* (default `io.netty.buffer.PooledByteBufAllocator`)
  

#### Configuration example

- `sink.json` example :
```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request"
    }
}
```
- `sink.json` example _with publishment in the in memory queue_, for the Source Connector:
```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "publish.to.in.memory.queue":"true"
    }
}
```

- a more advanced example :

```json 
{
  "name": "my-http-sink-connector",
  "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
    "value.converter.use.optional.for.nonrequired": "true",
    "value.converter.schema.registry.url": "https://my-schema-registry-url:8081",
    "errors.deadletterqueue.context.headers.enable": true,
    "errors.deadletterqueue.topic.name": "http-request-dlq-v1",
    "errors.log.enable": true,
    "errors.tolerance": "all",
    "generate.missing.request.id": "true",
    "generate.missing.correlation.id": "true",
    "publish.to.in.memory.queue":"true",
    "config.default.retries":"3",
    "config.default.retry.delay.in.ms":"2000",
    "config.default.retry.max.delay.in.ms":"600000",
    "config.default.retry.delay.factor":"1.5",
    "config.default.retry.jitter.in.ms":"500",
    "config.default.rate.limiter.period.in.ms":"1000",
    "config.default.rate.limiter.max.executions":"5",
    "config.default.retry.policy.response.code.regex":"^5[0-9][0-9]$",
    "config.default.success.response.code.regex":"^[1-2][0-9][0-9]$",
    "httpclient.ssl.truststore.path": "/path/to/my.truststore.jks",
    "httpclient.ssl.truststore.password": "mySecret_Pass",
    "httpclient.ssl.truststore.type": "jks"
  }
}
```

You can create or update this connector instance with this command :

```bash
curl -X PUT -H "Content-Type: application/json" --data @sink.json http://my-kafka-connect-cluster:8083/connectors/my-http-sink-connector/config
```
### HTTP Source Connector

The HTTP Source connector is only useful if you have configured the publishment of HTTP interactions into the queue,
via the `publish.to.in.memory.queue` set to `true`.

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

### Http Sink and Source Connectors

#### are linked

HTTP Sink connector can be used without the HTTP Source connector, if the `publish.to.in.memory.queue` parameter is set to `false`, and only this connector is configured.

HTTP Source Connector cannot be used without the HTTP Sink connector.

#### instantiated in the same location

When Http Sink and Source connectors are configured, they must be **instantiated in the same place**, to exchange data through the in memory queue.
The above configuration permits to fullfill these needs.

#### in the same classloader

Kafka Connect, loads connector plugins in isolation : each zip owning a plugin and its dependencies,
are loaded with a dedicated classloader, to avoid any dependencies conflicts.

To avoid any isolation issue between the Http Sink and Source plugin (to permit to exchange data via the in memory queue),
and to ease the install process, we ship them in the same jar (contained in the same zip archive).
So, any Http **Sink** connector, will have the ability to exchange data with the **Source** connector in the same classloader.
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

you have to configure a *tasks.max* parameter for each connector : the kafka connect cluster will distribute partitions among tasks.
Each connector instance will handle up to the `tasks.max` configuration parameter.
Kafka connect distributes tasks among workers, which are processes that execute connectors and tasks.

To avoid any issue, you must configure the same `tasks.max` parameters for the HTTP Sink and Source connectors.

### per site parameters


#### default configuration

Http Client is driven by some specific parameters, and a configuration, which owns :
- a rate limiter
- a success response code regex
- a retry response code regex
- a retry policy


There is a _default_ configuration.

#### custom configurations
But you can override this configuration, for some specific HTTP requests.


Each specific configuration is identified by a _prefix_, in the form : 
`config.<configurationid>.`

The prefix for the _default_ configuration is `httpclient.default.` (its configuration id is `default`).

To register some custom configurations, you need to register them by their id in the parameter :
`config.custom.config.ids`.
These configuration ids are separated by commas.

exemple : 
```
"config.custom.config.ids": "id1,id2,stuff"
```

for example, if you've registered a configuration with the id `test2`, it needs to be present in the field `httpclient.custom.config.ids`.

You must register a **predicate** to detect the matching between the http request and the configuration.
A predicate can be composed of multiple matchers (composed with a logical AND), configured with these parameters : 
- `config.test2.url.regex`
- `config.test2.method.regex`
- `config.test2.bodytype.regex`
- `config.test2.header.key.regex`
- `config.test2.header.value.regex` (can be added, only if the previous parameter is also configured)

You will have the ability to define optionnaly :
- a **rate limiter** with the parameters :
  - `config.test2.rate.limiter.max.executions`
  - `config.test2.rate.limiter.period.in.ms`
  - a **success response code regex** with the parameter : `httpclient.test2.success.response.code.regex`
  - a **retry response code regex** with the parameter : `httpclient.test2.retry.policy.response.code.regex`
  - a **retry policy** with the parameters : 
    - `config.test2.retries`
    - `config.test2.retry.delay.in.ms`
    - `config.test2.retry.max.delay.in.ms`
    - `config.test2.retry.delay.factor`
    - `config.test2.retry.jitter.in.ms`
