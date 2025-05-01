# Http Sink Connector settings

Here are the settings to setup an HTTP Sink connector into a Kafka Connect cluster. 

## required parameters

every Kafka Connect Sink Connector need to define these required parameters :

- *`connector.class`* : `io.github.clescot.kafka.connect.http.sink.HttpSinkConnector`
- *`topics`* (or *`topics.regex`*): `http-requests` for example

## optional common Kafka Connect parameters

- *`tasks.max`*  (default to `1`)
- *`key.converter`*
- *`value.converter`*
- ....
## optional Http Sink connector parameters

### export metrics via a Metrics Registry

metrics registry can be configured to add some metrics, and to export them. Metrics registry is global to the JVM.

Both exports (JMX and Prometheus) can be combined.

- JMX export
  you need to activate this export with :
  `"meter.registry.exporter.jmx.activate": "true"`


- prometheus export
  you need to activate this export with :
  `"meter.registry.exporter.prometheus.activate": "true"`
  by default, the port open is the default prometheus one (`9090`), but you can define yours with this setting :
  `"meter.registry.exporter.prometheus.port":"9087`


### expose some HTTP metrics

Only _okhttp_ HTTP client (default client) support this feature.

#### add some HTTP metrics
When at least one of the export is activated (JMX or Prometheus), a listener is added to the okhttp to expose some metrics as timers :
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

#### add other metrics

Some built-ins metrics are available :
- executor services metrics can be activated with `meter.registry.bind.metrics.executor.service` set to `true`
- _JVM memory_ metrics can be activated with `meter.registry.bind.metrics.jvm.memory` set to `true`
- _JVM threads_ metrics can be activated with `meter.registry.bind.metrics.jvm.thread` set to `true`
- _JVM info_ metrics can be activated with `"meter.registry.bind.metrics.jvm.info"` set to `true`
- _JVM Garbage Collector_ (GC) metrics can be activated with `"meter.registry.bind.metrics.jvm.gc"` set to `true`
- _JVM classloaders_ metrics can be activated with `"meter.registry.bind.metrics.jvm.classloader"` set to `true`
- _JVM processors_ metrics can be activated with `"meter.registry.bind.metrics.jvm.processor"` set to `true`
- _Logback_ metrics can be activated with `"meter.registry.bind.metrics.logback"` set to `true`


### Message splitters
Sometimes, we need to split value from one message, into multiple messages.
It can be done by configuring some message splitters. there is no default message splitter.
- `id`
  You need initially, to define one or multiple message splitter ids with this setting : 
  `"message.splitter.ids": "id1,id2"`
- `pattern` (configured with `message.splitter.id1.pattern`)
  define the delimiting java regex pattern used to split the message body into multiple ones.
- `limit` (configured with `message.splitter.id1.limit`)
  define the result threshold, like in the java split method.
- `matcher` (configured with `message.splitter.id1.matcher`)
  define the JEXL matching expression with identifies which message to split

### HttpRequestMapper

#### general

An HttpRequestMapper permits to map a message to an `HttpRequest` object.
There are 2 HttpRequestMapper modes available : `DIRECT` or `JEXL`.

A Matcher setting, common to the `DIRECT` and the `JEXL` mappers, is a JEXL expression, related to the message identified
by the variable `sinkRecord`.

#### type of HttpRequestMapper


##### `DIRECT` HttpRequestMapper
  This HttpRequestMapper implementation implies that it parses a direct serialization of an HttpRequest object.
  It requires a 'JEXL' matching expression to evaluates if it matches a message.

example :
- `http.request.mapper.myid1.matcher:'sinkRecord.topic()=='myTopic''`

##### `JEXL` HttpRequestMapper
  This HttpRequestMapper implementation implies that we need to build an HttpRequest object with some httpParts of the message.
If the body of the kafka message from the topic `test` consumed is in the format :
`url#method#body` (`http://mywebsite.com/path1#POST#body1`), we can configure the JEXL mapper :

- `http.request.mapper.default.mode: 'JEXL'`
- `http.request.mapper.default.url: 'sinkRecord.value().split("#")[0]'`
- `http.request.mapper.default.method: 'sinkRecord.value().split("#")[1]'`
- `http.request.mapper.default.body: 'sinkRecord.value().split("#")[2]'`

JEXL HttpRequestMapper `bodytype` is by default `STRING` (the only option supported now).
JEXL HttpRequestMapper headers ca be added as a constant with this setting (for a multi-valued :`value1` and `value2`, `test1` key):

`http.request.mapper.default.headers:"{'test1':['value1','value2',...]}"`

#### default HttpRequestMapper

The connector ships with a `default` HttpRequestMapper, which maps all messages by default 
(with a default catch-all message matcher on all topics), and we can, if needed, 
configure more HttpRequestMappers (with some specific message matchers).

The default mode is configured with `http.request.mapper.default.mode` set to `DIRECT` or `JEXL`.
If not configured, the default mode is 'DIRECT'.

It is useful to use a catch-all HttpRequestMapper to avoid to not process some messages. So, we don't recommand to change
the matcher of the default mapper (by default, the setting `http.request.mapper.default.matcher` is configured with the
JEXL `true` expression and used if no other mapper matches the message).


#### additional HttpRequestMappers

Additional HttpRequestMapper instances are configured with for each one a unique id set with `http.request.mapper.ids` 
(a list divided by a comma).
example : 
`http.request.mapper.ids: 'myid1,myid2'`
 

You can specify the topic of a DIRECT matcher mapping the `myTopic` messages with :
- `http.request.mapper.myid1.mode:'DIRECT'`
- `http.request.mapper.myid1.matcher:'sinkRecord.topic()=='myTopic''` 
 
### http request groupers
permit to regroup some http requests, into one http request with a body containing the grouped content.
there is no default http request grouper.
- `id`
  You need initially, to define some httpRequestGrouper ids with this setting :
  `"request.grouper.ids":"id1,id2,id3"`
- HttpRequest Predicate built with one or more settings:
  - `predicate.url.regex`
  - `predicate.method.regex`
  - `predicate.bodytype.regex`
  - `predicate.header.key.regex`
  - `predicate.header.value.regex`
- `separator` : define the string to insert between body httpParts from requests
- `start` : define the string to insert at the start of the body
- `end` : define the string to insert at the end of the body
- `message.limit` : define the maximum of initial messages grouped into one.
- `body.limit` : define the maximum body length in bytes of the grouped HttpRequest.

### Configuration

Configuration of an Http Client instance.


#### default configuration

Http Client is driven by a configuration, which owns :
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

The connector ships with a `default` configuration, and we can, if needed, configure more configurations.
A configuration is identified with a unique `id`.

Configurations are listed by their id, divided by comma (`,`), with the property : `config.ids`.

example : ```"config.ids":"config1,config2,config3"```.

Note that the `default` configuration is always created, and must not be referenced in the `config.ids` property.


For example, the `test4` configuration will have all its settings starting with the `config.test4` prefix,
following by other prefixes listed above. 

#### predicate
A configuration apply to some http requests based on a predicate  : all http requests not managed by a configuration are catch by the `default` configuration)
All the settings of the predicate, are starting with the `config.<configurationId>.predicate`.
The predicate permits to filter some http requests, and can be composed, cumulatively, with : 

- an _URL_ regex with the settings for the `default` configuration : ```"config.default.predicate.url.regex":"myregex"``` 
- a _method_ regex with the settings for the `default` configuration : ```"config.default.predicate.method.regex":"myregex"``` 
- a _bodytype_ regex with the settings for the `default` configuration : ```"config.default.predicate.bodytype.regex":"myregex"``` 
- a _header key_ with the settings for the `default` configuration (despite any value, when alone) : ```"config.default.predicate.header.key":"myheaderKey"```
- a _header value_ with the settings for the `default` configuration (can only be configured with a header key setting) : ```"config.default.predicate.header.value":"myheaderValue"```

- a configuration can enrich HTTP request and response by : 
  - adding static headers with the settings for `default` configuration : 
  ``` 
   "config.default.enrich.request.static.header.names":"header1,header2"
   "config.default.enrich.request.static.header.header1":"value1"
   "config.default.enrich.request.static.header.header2":"value2"
  ```
  
  - generating a missing correlationId  with the settings for the `default` configuration : ``` "config.default.enrich.request.generate.missing.correlation.id":true ```
  - generating a missing requestId with the settings for the `default` configuration : ``` "config.default.enrich.request.generate.missing.request.id":true ```
  - *`config.default.enrich.request.useragent.override.with`* (default `http_client`, and can be set to `project` or `custom`) :
    - `http_client` will let the http client implementation set the user-agent header (`okhttp/4.11.0` for okhttp).
    - `project` will set : `Mozilla/5.0 (compatible;kafka-connect-http/<version>; okhttp; https://github.com/clescot/kafka-connect-http)`, according to the [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html#name-the-user-agent-line)
    - `custom` will set the value bound to the `config.default.okhttp.interceptor.useragent.custom.value` parameter
  - *`config.default.enrich.request.useragent.custom.values`*  : values used if `config.default.enrich.request.useragent.override.with` is set
    to `custom`. If multiple values are provided (with `|` separator), code will pick randomly the value to use for each query.
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
    - *`config.default.retry.policy.retries`* : if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how many retries before an error is thrown
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
       note that the random generator configured in the configuration is used by the digest algorithm. 
      - *`config.default.httpclient.authentication.digest.activate`* : activate `digest` authentication response with credentials, for web sites matching this configuration (_required_)
      - *`config.default.httpclient.authentication.digest.username`* : username used to authenticate against the `digest` challenge (_required_)
      - *`config.default.httpclient.authentication.digest.password`* : password used to authenticate against the `digest` challenge (_required_)
      - *`config.default.httpclient.authentication.digest.charset`* : character set used by the http client to encode `digest` credentials (_optional_ `US-ASCII` if not set)
    - `OAuth2 client credential flow` authentication settings
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.activate`* : activate the OAuth2 Client Credentials flow authentication, suited for Machine-To-Machine applications (M2M).
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.well.known.url`* : OAuth2 URL of the provider's Well-Known Configuration Endpoint.
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.client.authentication.method`* : OAuth2 Client authentication method. either 'client_secret_basic', 'client_secret_post', or 'client_secret_jwt' are supported. default value is 'client_secret_basic'.
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.client.id`* : Client id : used in 'client_secret_basic', 'client_secret_post', or 'client_secret_jwt' authentication methods.
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.client.secret`* : Client secret : used in 'client_secret_basic', 'client_secret_post', or 'client_secret_jwt' authentication methods.
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.client.issuer`* : Client issuer for JWT token.
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.client.jws.algorithm`* : JWS Algorithm for JWT token. default is 'HS256'.
      - *`config.default.httpclient.authentication.oauth2.client.credentials.flow.scopes`* : optional scopes, splitted with a comma separator : can be used in  'client_secret_basic', 'client_secret_post', or 'client_secret_jwt' authentication methods.
      - 
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
  - http client HTTP Response settings
    - *`config.default.http.response.status.message.limit`*: Integer.MAX_VALUE if not set. truncate the status message to this length (to protect the HTTP Client instance).
    - *`config.default.http.response.body.limit`*: Integer.MAX_VALUE if not set. truncate the body to this length (to protect the HTTP Client instance).
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
    - *`config.default.httpclient.secure.random.activate`* : use a secure random generator if set to `true`. 
    - *`config.default.httpclient.secure.random.prng.algorithm`* : algorithm to use when the secure random is activated. default to `SHA1PRNG`. 
    - *`config.default.httpclient.unsecure.random.seed`* : seed to use when the secure random is NOT activated (default pseudo random generator is used). 
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
    - *`config.default.okhttp.retry.on.connection.failure`* : does the http client need to retry to establish a connection on failure. default to true.
    - *`config.default.okhttp.cache.activate`* (`true` to activate, `false` by default)
    - *`config.default.okhttp.cache.max.size`* (default `10000` max cache entries)
    - *`config.default.okhttp.cache.directory.path`* (default `/tmp/kafka-connect-http-cache` directory path for `file` type, default `/kafka-connect-http-cache` for `inmemory` type)
    - *`config.default.okhttp.cache.type`* (default `file`, and can be set to `inmemory`)
    - *`config.default.okhttp.interceptor.logging.activate`* (default `true`, and can be set to `false`) : trace in the output (at the debug level) Http request and response
    - *`config.default.okhttp.interceptor.inet.address.activate`* (default `false`, and can be set to `true`) : : add in the HttpResponse some additional Headers : 
      - `X-Host-Address` for the remote host address (for example, `87.248.100.215`)
      - `X-Host-Name` for the remote host name (for example, `www.yahoo.com`)
      - `X-Canonical-Host-Name` for the remote canonical host name (for example, `media-router-fp73.prod.media.vip.ir2.yahoo.com`)
    - *`config.default.okhttp.interceptor.ssl.handshake.activate`* (default `false`, and can be set to `true`) : display at the debug level, informations about the SSL handshake :
      - local principal
      - local certificates
      - remote principal
      - remote certificates
      - cipherSuite name
    - *`config.default.okhttp.doh.activate`* (default `false`, and can be set to `true`): activate Dns Over Https (DoH) feature
    - *`config.default.okhttp.doh.bootstrap.dns.hosts`* : dns hosts used to resolve DoH URL. if not set, use system dns.
    - *`config.default.okhttp.doh.url`* : url to resolve with https the host requested
    - *`config.default.okhttp.doh.include.ipv6`* : (default `true`, and can be set to `false`) : if set to `true`, include in the response the ipv6 httpPart if available.
    - *`config.default.okhttp.doh.use.post.method`* : (default `false`, and can be set to `true`) : by default, host resolution is done with a `GET` HTTPS request. if set to `true`, use a `POST` HTTPS request.
    - *`config.default.okhttp.doh.resolve.private.addresses`* : (default `false`, and can be set to `true`) : if set to `true`, resolve private adresses (cf https://www.rfc-editor.org/rfc/rfc6762#appendix-G).
    - *`config.default.okhttp.doh.resolve.public.addresses`* : (default `true`, and can be set to `false`) : if set to `true`, resolve public adresses.
  - _Async Http Client (AHC)_ implementation settings
    - *`org.asynchttpclient.http.max.connections`* :  (default `3`)
    - *`org.asynchttpclient.http.rate.limit.per.second`* (default `3`)
    - *`org.asynchttpclient.http.max.wait.ms`* (default `500 ms`)
    - *`org.asynchttpclient.keep.alive.class`* (default `org.asynchttpclient.channel.DefaultKeepAliveStrategy`)
    - *`org.asynchttpclient.response.body.httpPart.factory`* (default `EAGER`)
    - *`org.asynchttpclient.connection.semaphore.factory`* (default `org.asynchttpclient.netty.channel.DefaultConnectionSemaphoreFactory`)
    - *`org.asynchttpclient.cookie.store`* (default `org.asynchttpclient.cookie.ThreadSafeCookieStore`)
    - *`org.asynchttpclient.netty.timer`* (default `io.netty.util.HashedWheelTimer`)
    - *`org.asynchttpclient.byte.buffer.allocator`* (default `io.netty.buffer.PooledByteBufAllocator`)




### Configuration example

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
    "errors.deadletterqueue.context.headers.enable": "true",
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
    "config.default.httpclient.ssl.truststore.path": "/path/to/my.truststore.jks",
    "config.default.httpclient.ssl.truststore.password": "mySecret_Pass",
    "config.default.httpclient.ssl.truststore.type": "jks"
  }
}
```

#### publish mode
controlled by the  *`publish.mode`* parameter : `NONE` by default. When set to another value (`IN_MEMORY_QUEUE`,`PRODUCER`), publish HTTP interactions (request and responses)
- *`publish.mode`* parameter : `IN_MEMORY_QUEUE` publish into the _in memory_ queue, with a topology constraint : the source connector which consumes the in memory queue, must be present on the same kafka connect instance.
  - *`queue.name`* : if not set, `default` queue name is used, if the `publish.to.in.memory.queue` is set to `true`.
    You can define multiple in memory queues, to permit to publish to different topics, different HTTP interactions. If
    you set this parameter to a value different than `default`, you need to configure an HTTP source Connector listening
    on the same queue name to avoid some OutOfMemoryErrors.
  - *`wait.time.registration.queue.consumer.in.ms`* : wait time for a queue consumer (Source Connector) registration. default value is 60 seconds.
  - *`poll.delay.registration.queue.consumer.in.ms`* : poll delay, i.e, wait time before start polling a registered consumer. default value is 2 seconds.
  - *`poll.interval.registration.queue.consumer.in.ms`* : poll interval, i.e, time between every poll for a registered consumer. default value is 5000 milliseconds.
- *`publish.mode`* parameter : `PRODUCER` : use a low level kafka producer to publish HttpExchange in a dedicated topic. No topology constraint is required,
  but unlike other connectors (kafka connect handle that for us and hide it), we must configure the low level connection details. All settings starting with the prefix `producer.`
  will be passed to the producer instance to configure it.
  - `producer.bootstrap.servers` : required parameter to contact the kafka cluster.
  - `producer.topic` : name of the topic to publish httpExchange instances.
  - `producer.missing.id.cache.ttl.sec` : parameter configuration of the _CachedSchemaRegistryClient_
  - `producer.missing.version.cache.ttl.sec` : parameter configuration of the _CachedSchemaRegistryClient_
  - `producer.missing.schema.cache.ttl.sec` : parameter configuration of the _CachedSchemaRegistryClient_
  - `producer.missing.schema.cache.size` : parameter configuration of the _CachedSchemaRegistryClient_
  - `producer.bearer.auth.cache.expiry.buffer.seconds` : parameter configuration of the _CachedSchemaRegistryClient_
  - `producer.bearer.auth.scope.claim.name` : parameter configuration of the _CachedSchemaRegistryClient_
  - `producer.bearer.auth.sub.claim.name` : parameter configuration of the _CachedSchemaRegistryClient_
  - `producer.content` : can be `exchange` (default) or `response`. define if the complete HttpExchange is published (with request, response, and other data like timing), or only the HttpResponse.
  - other parameters can be passed to the low level kafka producer instance.

You can create or update this connector instance with this command :

```bash
curl -X PUT -H "Content-Type: application/json" --data @sink.json http://my-kafka-connect-cluster:8083/connectors/my-http-sink-connector/config
```
