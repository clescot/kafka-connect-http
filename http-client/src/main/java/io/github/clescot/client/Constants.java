package io.github.clescot.client;


public class Constants {



    //http client prefix
    public static final String HTTP_CLIENT_PREFIX = "httpclient.";
    public static final String PROXY_PREFIX = "proxy.";
    public static final String PROXYSELECTOR_PREFIX = "proxyselector.";
    public static final String OKHTTP_PREFIX = "okhttp.";
    public static final String AHC_PREFIX = "ahc.";

    public static final String DEFAULT_CONFIGURATION_PREFIX = "config.default.";


    //meter registry
    public static final String METER_REGISTRY_EXPORTER_JMX_ACTIVATE = "meter.registry.exporter.jmx.activate";
    public static final String METER_REGISTRY_EXPORTER_JMX_ACTIVATE_DOC = "activate exposure of metrics via JMX";

    public static final String METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE = "meter.registry.exporter.prometheus.activate";
    public static final String METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE_DOC = "activate exposure of metrics via prometheus";

    public static final String METER_REGISTRY_EXPORTER_PROMETHEUS_PORT = "meter.registry.exporter.prometheus.port";
    public static final String METER_REGISTRY_EXPORTER_PROMETHEUS_PORT_DOC = "define the port to use for prometheus exposition.";

    public static final String METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE = "meter.registry.bind.metrics.executor.service";
    public static final String METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE_DOC = "bind executor service metrics into registry";

    public static final String METER_REGISTRY_BIND_METRICS_JVM_MEMORY = "meter.registry.bind.metrics.jvm.memory";
    public static final String METER_REGISTRY_BIND_METRICS_JVM_MEMORY_DOC = "bind jvm memory metrics into registry";

    public static final String METER_REGISTRY_BIND_METRICS_JVM_THREAD = "meter.registry.bind.metrics.jvm.thread";
    public static final String METER_REGISTRY_BIND_METRICS_JVM_THREAD_DOC = "bind jvm thread metrics into registry";

    public static final String METER_REGISTRY_BIND_METRICS_JVM_INFO = "meter.registry.bind.metrics.jvm.info";
    public static final String METER_REGISTRY_BIND_METRICS_JVM_INFO_DOC = "bind jvm info metrics into registry";

    public static final String METER_REGISTRY_BIND_METRICS_JVM_GC = "meter.registry.bind.metrics.jvm.gc";
    public static final String METER_REGISTRY_BIND_METRICS_JVM_GC_DOC = "bind jvm garbage collector (GC) metrics into registry";

    public static final String METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER = "meter.registry.bind.metrics.jvm.classloader";
    public static final String METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER_DOC = "bind jvm classloader metrics into registry";

    public static final String METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR = "meter.registry.bind.metrics.jvm.processor";
    public static final String METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR_DOC = "bind jvm processor metrics into registry";

    public static final String METER_REGISTRY_BIND_METRICS_LOGBACK = "meter.registry.bind.metrics.logback";
    public static final String METER_REGISTRY_BIND_METRICS_LOGBACK_DOC = "bind logback metrics into registry";

    public static final String METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST = "meter.registry.tag.include.legacy.host";
    public static final String METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST_DOC = "include the legacy tag 'host'. host is already present in the 'target.host' tag.";

    public static final String METER_REGISTRY_TAG_INCLUDE_URL_PATH = "meter.registry.tag.include.url.path";
    public static final String METER_REGISTRY_TAG_INCLUDE_URL_PATH_DOC = "include the legacy tag 'host'. host is already present in the 'target.host' tag.";


    //message splitter
    public static final String MESSAGE_SPLITTER_IDS = "message.splitter.ids";
    public static final String MESSAGE_SPLITTER_IDS_DOC = "custom message splitter id list. no splitter is registered by default.";


    //request grouper
    public static final String REQUEST_GROUPER_PREFIX = "request.grouper.";
    public static final String REQUEST_GROUPER_IDS = REQUEST_GROUPER_PREFIX + "ids";
    public static final String REQUEST_GROUPER_IDS_DOC = "custom request grouper id list. no request grouper is registered by default.";

    //mapper
    public static final String HTTP_REQUEST_MAPPER_IDS = "http.request.mapper.ids";
    public static final String HTTP_REQUEST_MAPPER_IDS_DOC = "custom httpRequestMapper id list. 'default' http request mapper is already registered.";

    public static final String DEFAULT_REQUEST_MAPPER_PREFIX = "http.request.mapper.default.";
    public static final String REQUEST_MAPPER_DEFAULT_MODE = "mode";
    public static final String REQUEST_MAPPER_DEFAULT_MODE_DOC = "either 'direct' or 'jexl'. default is 'direct'.";

    public static final String REQUEST_MAPPER_DEFAULT_URL_EXPRESSION = "url";
    public static final String REQUEST_MAPPER_DEFAULT_URL_EXPRESSION_DOC = "a valid JEXL url expression to feed from the message the HttpRequest url field";

    public static final String REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION = "method";
    public static final String REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION_DOC = "a valid JEXL method expression to feed from the message the HttpRequest method field";

    public static final String REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION = "bodytype";
    public static final String REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION_DOC = "a valid JEXL method expression to feed from the message the HttpRequest bodyType field";

    public static final String REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION = "body";
    public static final String REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION_DOC = "a valid JEXL method expression to feed from the message the HttpRequest body field";

    public static final String REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION = "headers";
    public static final String REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION_DOC = "a valid JEXL method expression to feed from the message the HttpRequest headers field";

    public static final String REQUEST_MAPPER_DEFAULT_SPLIT_PATTERN = "split.pattern";
    public static final String REQUEST_MAPPER_DEFAULT_SPLIT_PATTERN_DOC = "a valid Regex Pattern (https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/regex/Pattern.html) to split the string body from message into multiple bodies (one per resulting HttpRequest)";

    public static final String REQUEST_MAPPER_DEFAULT_SPLIT_LIMIT = "split.limit";
    public static final String REQUEST_MAPPER_DEFAULT_SPLIT_LIMIT_DOC = "the number of times the pattern is applied, according to the split function from the java.util.regex.Pattern class (https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/regex/Pattern.html). can be positive, zero, or negative.";

    //message status limit
    public static final String HTTP_COOKIE_POLICY = HTTP_CLIENT_PREFIX + "cookie.policy";
    public static final String CONFIG_DEFAULT_HTTP_COOKIE_POLICY = DEFAULT_CONFIGURATION_PREFIX + HTTP_COOKIE_POLICY;
    public static final String CONFIG_DEFAULT_HTTP_COOKIE_POLICY_DOC = "define the cookie policy. accepted values are 'ACCEPT_NONE','ACCEPT_ORIGINAL_SERVER', or 'ACCEPT_ALL' (the default value)";

    //retry policy
    public static final String RETRY_POLICY_PREFIX = "retry.policy.";

    //default values
    public static final int DEFAULT_RETRIES_VALUE = 1;
    public static final long DEFAULT_RETRY_DELAY_IN_MS_VALUE = 2000L;
    public static final long DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE = 20000L;
    public static final double DEFAULT_RETRY_DELAY_FACTOR_VALUE = 1.5d;
    public static final long DEFAULT_RETRY_JITTER_IN_MS_VALUE = 500;


    public static final String RETRIES = RETRY_POLICY_PREFIX + "retries";
    public static final String CONFIG_DEFAULT_RETRIES = DEFAULT_CONFIGURATION_PREFIX + RETRIES;
    public static final String CONFIG_DEFAULT_RETRIES_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how many retries before an error is thrown";

    public static final String RETRY_DELAY_IN_MS = RETRY_POLICY_PREFIX + "retry.delay.in.ms";
    public static final String CONFIG_DEFAULT_RETRY_DELAY_IN_MS = DEFAULT_CONFIGURATION_PREFIX + RETRY_DELAY_IN_MS;
    public static final String CONFIG_DEFAULT_RETRY_DELAY_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long wait initially before first retry";

    public static final String RETRY_MAX_DELAY_IN_MS = RETRY_POLICY_PREFIX + "retry.max.delay.in.ms";
    public static final String CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS = DEFAULT_CONFIGURATION_PREFIX + RETRY_MAX_DELAY_IN_MS;
    public static final String CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long max wait before retry";

    public static final String RETRY_DELAY_FACTOR = RETRY_POLICY_PREFIX + "retry.delay.factor";
    public static final String CONFIG_DEFAULT_RETRY_DELAY_FACTOR = DEFAULT_CONFIGURATION_PREFIX + RETRY_DELAY_FACTOR;
    public static final String CONFIG_DEFAULT_RETRY_DELAY_FACTOR_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define the factor to multiply the previous delay to define the current retry delay";

    public static final String RETRY_JITTER_IN_MS = RETRY_POLICY_PREFIX + "retry.jitter.in.ms";
    public static final String CONFIG_DEFAULT_RETRY_JITTER_IN_MS = DEFAULT_CONFIGURATION_PREFIX + RETRY_JITTER_IN_MS;
    public static final String CONFIG_DEFAULT_RETRY_JITTER_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. " +
            "Define max entropy to add, to prevent many retry policies instances with the same parameters, to flood servers at the same time";

    //retry after settings
    public static final String RETRY_AFTER_MAX_DURATION_IN_SEC = RETRY_POLICY_PREFIX + "retry.after.max.duration.in.sec";
    public static final String CONFIG_DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC = DEFAULT_CONFIGURATION_PREFIX + RETRY_AFTER_MAX_DURATION_IN_SEC;
    public static final String CONFIG_DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC_DOC = "maximum duration in second for a retry-after header";
    public static final String DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC = "86400";

    public static final String RETRY_DELAY_THRESHOLD_IN_SEC = RETRY_POLICY_PREFIX + "retry.after.max.threshold.in.sec";
    public static final String CONFIG_DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC = DEFAULT_CONFIGURATION_PREFIX + RETRY_AFTER_MAX_DURATION_IN_SEC;
    public static final String CONFIG_DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC_DOC = "maximum delay threshold in second to consider retry-after header value. above this threshold, circuit breaker will be opened. under this threshold, a local wait will be done.";
    public static final String DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC = "60";

    public static final String DEFAULT_RETRY_DELAY_IN_SEC = RETRY_POLICY_PREFIX + "default.retry.after.delay.in.sec";
    public static final String CONFIG_DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC = DEFAULT_CONFIGURATION_PREFIX + RETRY_AFTER_MAX_DURATION_IN_SEC;
    public static final String CONFIG_DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC_DOC = "delay in second to use if retry-after header value is not set.";
    public static final String DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC = "3600";

    public static final String CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER = RETRY_POLICY_PREFIX + "retry.after.status.code";
    public static final String CONFIG_DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER = DEFAULT_CONFIGURATION_PREFIX + RETRY_AFTER_MAX_DURATION_IN_SEC;
    public static final String CONFIG_DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER_DOC = "maximum delay threshold in second to consider retry-after header value. above this threshold, circuit breaker will be opened. under this threshold, a local wait will be done.";
    public static final String DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER = "503|429|301";

    //rate limiter
    public static final String DEFAULT_RATE_LIMITER_ONE_PERMIT_PER_CALL = "one";
    public static final String RATE_LIMITER_REQUEST_LENGTH_PER_CALL = "request_length";

    //rate limiter period
    public static final String DEFAULT_RATE_LIMITER_PREFIX = "rate.limiter.";
    public static final String RATE_LIMITER_PERIOD_IN_MS = DEFAULT_RATE_LIMITER_PREFIX + "period.in.ms";
    public static final String CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS = DEFAULT_CONFIGURATION_PREFIX + RATE_LIMITER_PERIOD_IN_MS;
    public static final String CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS_DOC = "period of time in milliseconds, during the max execution cannot be exceeded";

    //rate limiter permits per call
    public static final String RATE_LIMITER_PERMITS_PER_EXECUTION = DEFAULT_RATE_LIMITER_PREFIX + "permits.per.execution";
    public static final String CONFIG_DEFAULT_RATE_LIMITER_PERMITS_PER_EXECUTION = DEFAULT_CONFIGURATION_PREFIX + RATE_LIMITER_PERMITS_PER_EXECUTION;
    public static final String CONFIG_DEFAULT_RATE_LIMITER_PERMITS_PER_EXECUTION_DOC = "how many permits are consumed per execution. default is '" + DEFAULT_RATE_LIMITER_ONE_PERMIT_PER_CALL + "', i.e one permit per call. if set to LENGTH, the number of permits consumed will be equal to the content length of the request body. if set to '" + RATE_LIMITER_REQUEST_LENGTH_PER_CALL + "', the number of permits consumed will be equal to the content length of the request body plus the size of all headers in bytes.";

    //rate limiter max executions
    public static final String RATE_LIMITER_MAX_EXECUTIONS = DEFAULT_RATE_LIMITER_PREFIX + "max.executions";
    public static final String CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS = DEFAULT_CONFIGURATION_PREFIX + RATE_LIMITER_MAX_EXECUTIONS;
    public static final String CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_DOC = "max executions (permits, i.e execution if one permit per execution) in the period defined with the '" + CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS + "' parameter";

    //rate limiter scope
    public static final String RATE_LIMITER_SCOPE = DEFAULT_RATE_LIMITER_PREFIX + "scope";
    public static final String CONFIG_DEFAULT_RATE_LIMITER_SCOPE = DEFAULT_CONFIGURATION_PREFIX + RATE_LIMITER_SCOPE;
    public static final String CONFIG_DEFAULT_RATE_LIMITER_SCOPE_DOC = "scope of the '" + CONFIG_DEFAULT_RATE_LIMITER_SCOPE + "' parameter. can be either 'instance' (i.e a rate limiter per configuration in the connector instance),  or 'static' (a rate limiter per configuration id shared with all connectors instances in the same Java Virtual Machine.";

    public static final long DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE = 1000L;
    public static final long DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE = 1L;
    public static final String DEFAULT_RATE_LIMITER_SCOPE_VALUE = "instance";


    //enrich HttpRequest
    public static final String ENRICH_REQUEST = "enrich.request.";
    public static final String STATIC_REQUEST_HEADER_PREFIX = ENRICH_REQUEST + "static.header.";
    public static final String STATIC_REQUEST_HEADER_NAMES = STATIC_REQUEST_HEADER_PREFIX + "names";
    public static final String CONFIG_STATIC_REQUEST_HEADER_NAMES = DEFAULT_CONFIGURATION_PREFIX + STATIC_REQUEST_HEADER_NAMES;
    public static final String CONFIG_STATIC_REQUEST_HEADER_NAMES_DOC = "list of static parameters names which will be added to all http requests. these parameter names need to be added with their values as parameters in complement of this list";


    public static final String GENERATE_MISSING_CORRELATION_ID = ENRICH_REQUEST + "generate.missing.correlation.id";
    public static final String CONFIG_GENERATE_MISSING_CORRELATION_ID = DEFAULT_CONFIGURATION_PREFIX + GENERATE_MISSING_CORRELATION_ID;
    public static final String CONFIG_GENERATE_MISSING_CORRELATION_ID_DOC = "if not present in the HttpRequest headers, generate an UUID bound to the 'X-Correlation-ID' name";

    public static final String GENERATE_MISSING_REQUEST_ID = ENRICH_REQUEST + "generate.missing.request.id";
    public static final String CONFIG_GENERATE_MISSING_REQUEST_ID = DEFAULT_CONFIGURATION_PREFIX + GENERATE_MISSING_REQUEST_ID;
    public static final String CONFIG_GENERATE_MISSING_REQUEST_ID_DOC = "if not present in the HttpRequest headers, generate an UUID bound to the 'X-Request-ID' name";

    public static final String USER_AGENT_OVERRIDE = ENRICH_REQUEST + "useragent.override.with";
    public static final String CONFIG_DEFAULT_USER_AGENT_OVERRIDE = DEFAULT_CONFIGURATION_PREFIX + USER_AGENT_OVERRIDE;
    public static final String CONFIG_DEFAULT_USER_AGENT_OVERRIDE_DOC = "activate 'User-Agent' header override. Accepted values are `http_client` will let the http client implementation set the user-agent header (okhttp/4.11.0 for okhttp).`project` will set : `Mozilla/5.0 (compatible;kafka-connect-http/<version>; okhttp; https://github.com/clescot/kafka-connect-http)`, according to the [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html#name-the-user-agent-line).`custom` will set the value bound to the `config.default.useragent.custom.value` parameter.";

    public static final String USER_AGENT_CUSTOM_VALUES = ENRICH_REQUEST + "useragent.custom.values";
    public static final String CONFIG_DEFAULT_USER_AGENT_CUSTOM_VALUES = DEFAULT_CONFIGURATION_PREFIX + USER_AGENT_CUSTOM_VALUES;
    public static final String CONFIG_DEFAULT_USER_AGENT_CUSTOM_VALUES_DOC = "custom values for the user-agent header. if multiple values are provided (with `|` separator), code will pick randomly the value to use.";

    //HttpResponse
    public static final String HTTP_RESPONSE = "http.response.";
    //message status limit
    public static final String HTTP_RESPONSE_MESSAGE_STATUS_LIMIT = HTTP_RESPONSE + "message.status.limit";
    public static final String CONFIG_DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT = DEFAULT_CONFIGURATION_PREFIX + HTTP_RESPONSE_MESSAGE_STATUS_LIMIT;
    public static final String CONFIG_DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT_DOC = "define the max length of the HTTP Response message status. 1024 is the default limit.";
    //headers limit
    public static final String HTTP_RESPONSE_HEADERS_LIMIT = HTTP_RESPONSE + "headers.limit";
    public static final String CONFIG_DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT = DEFAULT_CONFIGURATION_PREFIX + HTTP_RESPONSE_HEADERS_LIMIT;
    public static final String CONFIG_DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT_DOC = "define the max length of the HTTP Response headers. 10_000 is the default limit.";
    //body limit
    public static final String HTTP_RESPONSE_BODY_LIMIT = HTTP_RESPONSE + "body.limit";
    public static final String CONFIG_DEFAULT_HTTP_RESPONSE_BODY_LIMIT = DEFAULT_CONFIGURATION_PREFIX + HTTP_RESPONSE_BODY_LIMIT;
    public static final String CONFIG_DEFAULT_HTTP_RESPONSE_BODY_LIMIT_DOC = "define the max length of the HTTP Response message body. 100_000 is the default limit";

    //enrich exchange
    public static final String ENRICH_EXCHANGE = "enrich.exchange.";
    public static final String CONFIGURATION_IDS = "config.ids";
    public static final String CONFIGURATION_IDS_DOC = "custom configurations id list. 'default' configuration is already registered.";


    //enrich httpExchange
    public static final String SUCCESS_RESPONSE_CODE_REGEX = ENRICH_EXCHANGE + "success.response.code.regex";
    public static final String CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX = DEFAULT_CONFIGURATION_PREFIX + SUCCESS_RESPONSE_CODE_REGEX;
    public static final String CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX_DOC = "default regex which decide if the request is a success or not, based on the response status code";
    //by default, we don't resend any http call with a response between 100 and 499
    // 1xx is for protocol information (100 continue for example),
    // 2xx is for success,
    // 3xx is for redirection
    //4xx is for a client error
    //5xx is for a server error
    //only 5xx by default, trigger a resend

    /*
     *  HTTP Server status code returned
     *  3 cases can arise:
     *  * a success occurs : the status code returned from the ws server is matching the regexp => no retries
     *  * a functional error occurs: the status code returned from the ws server is not matching the regexp, but is lower than 500 => no retries
     *  * a technical error occurs from the WS server : the status code returned from the ws server does not match the regexp AND is equals or higher than 500 : retries are done
     */

    public static final String DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX = "^5[0-9][0-9]$";

    public static final String CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX = "^[1-2][0-9][0-9]$";
    public static final String RETRY_RESPONSE_CODE_REGEX = RETRY_POLICY_PREFIX + "response.code.regex";
    public static final String CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX = DEFAULT_CONFIGURATION_PREFIX + RETRY_RESPONSE_CODE_REGEX;
    public static final String DEFAULT_RETRY_RESPONSE_CODE_REGEX_DOC = "regex which define if a retry need to be triggered, based on the response status code. default is '" + CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX + "'";


    public static final String HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE = HTTP_CLIENT_PREFIX + "async.fixed.thread.pool.size";
    public static final String HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE_DOC = "custom fixed thread pool size used to execute asynchronously http requests.";

    public static final String FALSE = "false";
    public static final String TRUE = "true";



    //configuration


    //retry policy


    //rate limiter
    public static final String HTTP_CLIENT_IMPLEMENTATION = HTTP_CLIENT_PREFIX + "implementation";
    public static final String CONFIG_HTTP_CLIENT_IMPLEMENTATION = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_IMPLEMENTATION;
    public static final String CONFIG_HTTP_CLIENT_IMPLEMENTATION_DOC = "define which intalled library to use : either 'ahc', a.k.a async http client, or 'okhttp'. default is 'okhttp'.";

    public static final String OKHTTP_IMPLEMENTATION = "okhttp";
    public static final String AHC_IMPLEMENTATION = "ahc";

    //proxy
    public static final String PROXY_HTTP_CLIENT_HOSTNAME = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "hostname";
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_HOSTNAME = DEFAULT_CONFIGURATION_PREFIX + PROXY_HTTP_CLIENT_HOSTNAME;
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_HOSTNAME_DOC = "hostname of the proxy host.";

    public static final String PROXY_HTTP_CLIENT_PORT = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "port";
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_PORT = DEFAULT_CONFIGURATION_PREFIX + PROXY_HTTP_CLIENT_PORT;
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_PORT_DOC = "hostname of the proxy host.";

    public static final String PROXY_HTTP_CLIENT_TYPE = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "type";
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_TYPE = DEFAULT_CONFIGURATION_PREFIX + PROXY_HTTP_CLIENT_TYPE;
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_TYPE_DOC = "type of proxy. can be either 'HTTP' (default), 'DIRECT' (i.e no proxy), or 'SOCKS'";

    //proxy selector
    public static final String PROXY_SELECTOR_ALGORITHM = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "algorithm";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_ALGORITHM;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_ALGORITHM_DOC = "algorithm of the proxy selector.can be 'uriregex', 'random', 'weightedrandom', or 'hosthash'. Default is 'uriregex'.";


    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "hostname";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME_DOC = "hostname of the proxy host.";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_PORT = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "port";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_PORT = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_PORT;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_PORT_DOC = "hostname of the proxy host.";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_TYPE = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "type";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_TYPE = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_TYPE;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_TYPE_DOC = "type of proxy. can be either 'HTTP' (default), 'DIRECT' (i.e no proxy), or 'SOCKS'";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "uri.regex";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX_DOC = "uri regex matching this proxy";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "non.proxy.hosts.uri.regex";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX_DOC = "hosts which don't need to be proxied to be reached.";


    //okhttp settings
    //cache
    public static final String OKHTTP_CACHE_ACTIVATE = OKHTTP_PREFIX + "cache.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE_DOC = "set to true to activate page cache (if cache hit, the server will not receive the request, and the response will comes from the cache). default is false.";

    public static final String OKHTTP_CACHE_MAX_SIZE = OKHTTP_PREFIX + "cache.max.size";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_MAX_SIZE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE_DOC = "max size of the page cache.";

    public static final String OKHTTP_CACHE_TYPE = OKHTTP_PREFIX + "cache.type";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_TYPE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_TYPE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_TYPE_DOC = "persistance of the cache : either 'file'(default), or 'inmemory'.";

    public static final String OKHTTP_CACHE_DIRECTORY_PATH = OKHTTP_PREFIX + "cache.directory.path";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_DIRECTORY_PATH;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH_DOC = "file system path of the cache directory.";

    //DNS over HTTPS
    public static final String OKHTTP_DOH_PREFIX=OKHTTP_PREFIX +"doh.";

    public static final String OKHTTP_DOH_ACTIVATE=OKHTTP_DOH_PREFIX +".activate";
    public static final String OKHTTP_DOH_ACTIVATE_DOC="resolve DNS domain with HTTPS if set to 'true'";

    public static final String OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS=OKHTTP_DOH_PREFIX +".bootstrap.dns.hosts";
    public static final String OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS_DOC="list of bootstrap dns";

    public static final String OKHTTP_DOH_INCLUDE_IPV6=OKHTTP_DOH_PREFIX +".include.ipv6";
    public static final String OKHTTP_DOH_INCLUDE_IPV6_DOC="include ipv6. default is 'true'";

    public static final String OKHTTP_DOH_USE_POST_METHOD=OKHTTP_DOH_PREFIX +".use.post.method";
    public static final String OKHTTP_DOH_USE_POST_METHOD_DOC="use HTTP 'POST' method instead of get. default is 'false'.";

    public static final String OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES=OKHTTP_DOH_PREFIX +".resolve.private.addresses";
    public static final String OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES_DOC="resolve private addresses. default is 'false'";

    public static final String OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES=OKHTTP_DOH_PREFIX +".resolve.public.addresses";
    public static final String OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES_DOC="resolve public addresses. default is 'true'";

    public static final String OKHTTP_DOH_URL=OKHTTP_DOH_PREFIX +".url";
    public static final String OKHTTP_DOH_URL_DOC="DNS Over HTTP url";

    //connection
    public static final String OKHTTP_CALL_TIMEOUT = OKHTTP_PREFIX + "call.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CALL_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT_DOC = "default timeout in milliseconds for complete call . A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_READ_TIMEOUT = OKHTTP_PREFIX + "read.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_READ_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT_DOC = "Sets the default read timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_CONNECT_TIMEOUT = OKHTTP_PREFIX + "connect.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECT_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT_DOC = "Sets the default connect timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_WRITE_TIMEOUT = OKHTTP_PREFIX + "write.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_WRITE_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT_DOC = "Sets the default write timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION = OKHTTP_PREFIX + "ssl.skip.hostname.verification";
    public static final String CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION;
    public static final String CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION_DOC = "if set to 'true', skip hostname verification. Not set by default.";

    public static final String OKHTTP_RETRY_ON_CONNECTION_FAILURE = OKHTTP_PREFIX + "retry.on.connection.failure";
    public static final String CONFIG_DEFAULT_OKHTTP_RETRY_ON_CONNECTION_FAILURE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_RETRY_ON_CONNECTION_FAILURE;
    public static final String CONFIG_DEFAULT_OKHTTP_RETRY_ON_CONNECTION_FAILURE_DOC = "if set to 'false', will not retry connection on connection failure. default is true";


    //protocols to use, in order of preference,divided by a comma.supported protocols in okhttp: HTTP_1_1,HTTP_2,H2_PRIOR_KNOWLEDGE,QUIC
    public static final String OKHTTP_PROTOCOLS = OKHTTP_PREFIX + "protocols";
    public static final String CONFIG_DEFAULT_OKHTTP_PROTOCOLS = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_PROTOCOLS;
    public static final String CONFIG_DEFAULT_OKHTTP_PROTOCOLS_DOC = "the protocols to use, in order of preference. If the list contains 'H2_PRIOR_KNOWLEDGE' then that must be the only protocol and HTTPS URLs will not be supported. Otherwise the list must contain 'HTTP_1_1'. The list must not contain null or 'HTTP_1_0'.";

    public static final String OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION = OKHTTP_PREFIX + "connection.pool.keep.alive.duration";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION_DOC = "Time in milliseconds to keep the connection alive in the pool before closing it. Default is 0 (no connection pool).";

    public static final String OKHTTP_CONNECTION_POOL_SCOPE = OKHTTP_PREFIX + "connection.pool.scope";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_SCOPE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECTION_POOL_SCOPE;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_SCOPE_DOC = "scope of the '" + CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_SCOPE + "' parameter. can be either 'instance' (i.e a connection pool per configuration in the connector instance),  or 'static' (a connection pool shared with all connectors instances in the same Java Virtual Machine).";

    public static final String OKHTTP_DISPATCHER_MAX_REQUESTS = OKHTTP_PREFIX + "dispatcher.max.requests";
    public static final String CONFIG_DEFAULT_OKKHTTP_DISPATCHER_MAX_REQUESTS = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_DISPATCHER_MAX_REQUESTS;
    public static final String CONFIG_DEFAULT_OKKHTTP_DISPATCHER_MAX_REQUESTS_DOC="maximum number of requests to execute concurrently. Default is 64.";

    public static final String OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST = OKHTTP_PREFIX + "dispatcher.max.requests.per.host";
    public static final String CONFIG_DEFAULT_OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST;
    public static final String CONFIG_DEFAULT_OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST_DOC = "maximum number of requests for each host to execute concurrently. Default is 5.";

    public static final String OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS = OKHTTP_PREFIX + "connection.pool.max.idle.connections";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DOC = "amount of connections to keep idle, to avoid the connection creation time when needed. Default is 0 (no connection pool)";

    public static final String OKHTTP_FOLLOW_REDIRECT = OKHTTP_PREFIX + "follow.redirect";
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_FOLLOW_REDIRECT;
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT_DOC = "does the http client need to follow a redirect response from the server. default to true.";

    public static final String OKHTTP_FOLLOW_SSL_REDIRECT = OKHTTP_PREFIX + "follow.ssl.redirect";
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_FOLLOW_SSL_REDIRECT;
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT_DOC = "does the http client need to follow an SSL redirect response from the server. default to true.";

    public static final String OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE = OKHTTP_PREFIX + "interceptor.logging.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE_DOC = "activate tracing of request and responses via an okhttp network interceptor. 'true' and 'false' are accepted values. default is true";

    public static final String OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE = OKHTTP_PREFIX + "interceptor.inet.address.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE_DOC = "activate tracing of request and responses via an okhttp network interceptor. 'true' and 'false' are accepted values. default is true";

    public static final String OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE = OKHTTP_PREFIX + "interceptor.ssl.handshake.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE_DOC = "activate tracing of request and responses via an okhttp network interceptor. 'true' and 'false' are accepted values. default is true";

    //random
    public static final String HTTP_CLIENT_SECURE_RANDOM_ACTIVATE = HTTP_CLIENT_PREFIX + "secure.random.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SECURE_RANDOM_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_ACTIVATE_DOC = "if 'true', use a secure random instead of a pseudo random number generator.";


    public static final String HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM = HTTP_CLIENT_PREFIX + "secure.random.prng.algorithm";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM_DOC = "name of the Random Number Generator (RNG) algorithm used to get a Secure Random instance. if not set, 'SHA1PRNG' algorithm is used when the secure random generator is activated. cf https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#securerandom-number-generation-algorithms";

    public static final String HTTP_CLIENT_UNSECURE_RANDOM_SEED = HTTP_CLIENT_PREFIX + "unsecure.random.seed";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_UNSECURE_RANDOM_SEED = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_UNSECURE_RANDOM_SEED;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_UNSECURE_RANDOM_SEED_DOC = "seed used to build the unsecure random generator.";


    //SSL
    public static final String HTTP_CLIENT_SSL_KEYSTORE_PATH = HTTP_CLIENT_PREFIX + "ssl.keystore.path";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_PATH;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH_DOC = "file path of the custom key store.";

    public static final String HTTP_CLIENT_SSL_KEYSTORE_PASSWORD = HTTP_CLIENT_PREFIX + "ssl.keystore.password";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_PASSWORD;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD_DOC = "password of the custom key store.";

    public static final String HTTP_CLIENT_SSL_KEYSTORE_TYPE = HTTP_CLIENT_PREFIX + "ssl.keystore.type";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_TYPE;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE_DOC = "keystore type. can be 'jks' or 'pkcs12'.";

    public static final String HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM = HTTP_CLIENT_PREFIX + "ssl.keystore.algorithm";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM_DOC = "the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST = HTTP_CLIENT_PREFIX + "ssl.truststore.always.trust";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST_DOC = "trust store that always trust any certificate. this option remove any security on the transport layer. be careful when you activate this option ! you will have no guarantee that you don't contact any hacked server ! ";


    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_PATH = HTTP_CLIENT_PREFIX + "ssl.truststore.path";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_PATH;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH_DOC = "file path of the custom trust store.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD = HTTP_CLIENT_PREFIX + "ssl.truststore.password";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD_DOC = "password of the custom trusted store.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_TYPE = HTTP_CLIENT_PREFIX + "ssl.truststore.type";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_TYPE;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE_DOC = "truststore type. can be 'jks' or 'pkcs12'.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM = HTTP_CLIENT_PREFIX + "ssl.truststore.algorithm";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM_DOC = "the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.";

    //authentication
    //Basic
    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE = HTTP_CLIENT_PREFIX + "authentication.basic.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE_DOC = "activate the BASIC authentication";


    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME = HTTP_CLIENT_PREFIX + "authentication.basic.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_BASIC_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_BASIC_USER_DOC = "username for basic authentication";

    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD = HTTP_CLIENT_PREFIX + "authentication.basic.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD_DOC = "password for basic authentication";

    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET = HTTP_CLIENT_PREFIX + "authentication.basic.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET_DOC = "charset used to encode basic credentials. default is 'ISO-8859-1'";

    //Digest
    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE = HTTP_CLIENT_PREFIX + "authentication.digest.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE_DOC = "activate the DIGEST authentication";


    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME = HTTP_CLIENT_PREFIX + "authentication.digest.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_DIGEST_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_DIGEST_USER_DOC = "username for digest authentication";

    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD = HTTP_CLIENT_PREFIX + "authentication.digest.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD_DOC = "password for digest authentication";


    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET = HTTP_CLIENT_PREFIX + "authentication.digest.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET_DOC = "charset used to encode 'digest' credentials. default is 'US-ASCII'";

    //OAuth2
    //Client Credentials Flow
    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE_DOC = "activate the OAuth2 Client Credentials flow authentication, suited for Machine-To-Machine applications (M2M).";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.well.known.url";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL_DOC = "OAuth2 URL of the provider's Well-Known Configuration Endpoint.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.authentication.method";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD_DOC = "OAuth2 Client authentication method. either 'client_secret_basic', 'client_secret_post', or 'client_secret_jwt' are supported. default value is 'client_secret_basic'.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.id";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID_DOC = "Client id.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.secret";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET_DOC = "Client secret.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.issuer";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER_DOC = "Client issuer for JWT token.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.jws.algorithm";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM_DOC = "JWS Algorithm for JWT token. default is 'HS256'.";


    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.scopes";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES_DOC = "optional scopes, splitted with a comma separator.";


    //proxy authentication
    //Basic on proxy
    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE_DOC = "activate the BASIC authentication for proxy.";


    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_BASIC_USER_DOC = "username for proxy basic authentication";

    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD_DOC = "password for proxy basic authentication";

    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET_DOC = "charset used to encode basic credentialsfor proxy. default is 'ISO-8859-1'";

    //Digest on proxy
    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE_DOC = "activate the DIGEST authentication for proxy.";


    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_DIGEST_USER_DOC = "username for proxy digest authentication";

    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD_DOC = "password for proxy digest authentication";


    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET_DOC = "charset used to encode proxy 'digest' credentials. default is 'US-ASCII'";


}
