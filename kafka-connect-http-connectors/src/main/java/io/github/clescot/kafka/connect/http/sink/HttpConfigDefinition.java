package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.github.clescot.kafka.connect.ConfigUtils;
import io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition;
import io.github.clescot.kafka.connect.http.mapper.MapperMode;
import org.apache.kafka.common.config.ConfigDef;

import java.util.*;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;

public class HttpConfigDefinition {


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
    private static final int DEFAULT_RETRIES_VALUE = 1;
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

    //enrich httpExchange
    public static final String ENRICH_EXCHANGE = "enrich.exchange.";
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

    private final Map<String, String> settings;

    public HttpConfigDefinition(Map<String, String> settings) {
        this.settings = settings;
    }


    public ConfigDef config() {
        HttpClientConfigDefinition httpClientConfigDefinition = new HttpClientConfigDefinition(settings);
        ConfigDef configDef = httpClientConfigDefinition.config()
                //meter registry
                //exporters
                .define(METER_REGISTRY_EXPORTER_JMX_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_EXPORTER_JMX_ACTIVATE_DOC)
                .define(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE_DOC)
                .define(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT, ConfigDef.Type.INT, 9090, ConfigDef.Importance.LOW, METER_REGISTRY_EXPORTER_PROMETHEUS_PORT_DOC)
                //bind metrics
                .define(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_GC, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_GC_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_INFO, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_INFO_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_MEMORY, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_MEMORY_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_THREAD, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_THREAD_DOC)
                .define(METER_REGISTRY_BIND_METRICS_LOGBACK, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_LOGBACK_DOC)
                //tags
                .define(METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST_DOC)
                .define(METER_REGISTRY_TAG_INCLUDE_URL_PATH, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_TAG_INCLUDE_URL_PATH_DOC)

                .define(USER_AGENT_OVERRIDE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_OVERRIDE_DOC)
                .define(USER_AGENT_CUSTOM_VALUES, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_CUSTOM_VALUES_DOC)
                //async settings
                .define(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE, ConfigDef.Type.INT, null, ConfigDef.Importance.MEDIUM, HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE_DOC)

                //custom message splitters
                .define(MESSAGE_SPLITTER_IDS, ConfigDef.Type.LIST, Lists.newArrayList(), ConfigDef.Importance.LOW, MESSAGE_SPLITTER_IDS_DOC)
                //custom request groupers
                .define(REQUEST_GROUPER_IDS, ConfigDef.Type.LIST, Lists.newArrayList(), ConfigDef.Importance.LOW, REQUEST_GROUPER_IDS_DOC)
                //custom request mappers
                .define(HTTP_REQUEST_MAPPER_IDS, ConfigDef.Type.LIST, Lists.newArrayList(), ConfigDef.Importance.LOW, HTTP_REQUEST_MAPPER_IDS_DOC);
                SinkConfigDefinition sinkConfigDefinition = new SinkConfigDefinition();
                configDef = ConfigUtils.mergeConfigDefs(configDef, sinkConfigDefinition.config());

        //custom httpRequestmappers
        String httpRequestMapperIds = settings.get(HTTP_REQUEST_MAPPER_IDS);
        Set<String> mappers = Sets.newHashSet();
        if (httpRequestMapperIds != null) {
            mappers.addAll(Arrays.asList(httpRequestMapperIds.split(",")));
        }
        mappers.add("default");
        for (String httpRequestmapperName : mappers) {
            configDef = appendHttpRequestMapperConfigDef(configDef, httpRequestmapperName);
        }

        //DNS over HTTPS (DoH)
        configDef = configDef.define(OKHTTP_DOH_ACTIVATE, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.LOW, OKHTTP_DOH_ACTIVATE_DOC)
                .define(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS_DOC)
                .define(OKHTTP_DOH_INCLUDE_IPV6, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.LOW, OKHTTP_DOH_INCLUDE_IPV6_DOC)
                .define(OKHTTP_DOH_USE_POST_METHOD, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.LOW, OKHTTP_DOH_USE_POST_METHOD_DOC)
                .define(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.LOW, OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES_DOC)
                .define(OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.LOW, OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES_DOC)
                .define(OKHTTP_DOH_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, OKHTTP_DOH_URL_DOC);
        //custom configurations
        String configurationIds = settings.get(CONFIGURATION_IDS);
        Set<String> configs = Sets.newHashSet();
        if (configurationIds != null) {
            configs.addAll(Arrays.asList(configurationIds.split(",")));
        }
        configs.add("default");
        for (String configurationName : configs) {
            configDef = appendConfigurationConfigDef(configDef, configurationName);
        }
        return configDef;
    }

    private ConfigDef appendConfigurationConfigDef(ConfigDef configDef, String configurationName) {
        String prefix = "config." + configurationName + ".";
        ConfigDef configDef1 = configDef//http client implementation settings
                //cookie policy settings
                .define(prefix + HTTP_COOKIE_POLICY, ConfigDef.Type.STRING, CONFIG_DEFAULT_HTTP_COOKIE_POLICY, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_COOKIE_POLICY_DOC)
                //retry after settings
                .define(prefix + RETRY_AFTER_MAX_DURATION_IN_SEC,ConfigDef.Type.STRING, DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC_DOC)
                .define(prefix + RETRY_DELAY_THRESHOLD_IN_SEC,ConfigDef.Type.STRING, DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC_DOC)
                .define(prefix + DEFAULT_RETRY_DELAY_IN_SEC,ConfigDef.Type.STRING, DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC_DOC)
                .define(prefix + CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER,ConfigDef.Type.STRING, DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC_DOC)
                //retry settings
                .define(prefix + SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX_DOC)
                .define(prefix + RETRY_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, DEFAULT_RETRY_RESPONSE_CODE_REGEX_DOC)
                .define(prefix + RETRIES, ConfigDef.Type.INT, DEFAULT_RETRIES_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRIES_DOC)
                .define(prefix + RETRY_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_DELAY_IN_MS_DOC)
                .define(prefix + RETRY_MAX_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS_DOC)
                .define(prefix + RETRY_DELAY_FACTOR, ConfigDef.Type.DOUBLE, DEFAULT_RETRY_DELAY_FACTOR_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_DELAY_FACTOR_DOC)
                .define(prefix + RETRY_JITTER_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_JITTER_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_JITTER_IN_MS_DOC)
                //rate limiting settings
                .define(prefix + RATE_LIMITER_PERIOD_IN_MS, ConfigDef.Type.LONG, HttpConfigDefinition.DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS_DOC)
                .define(prefix + RATE_LIMITER_MAX_EXECUTIONS, ConfigDef.Type.LONG, HttpConfigDefinition.DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_DOC)
                .define(prefix + RATE_LIMITER_SCOPE, ConfigDef.Type.STRING, HttpConfigDefinition.DEFAULT_RATE_LIMITER_SCOPE_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_SCOPE_DOC)
                .define(prefix + RATE_LIMITER_PERMITS_PER_EXECUTION, ConfigDef.Type.STRING, HttpConfigDefinition.DEFAULT_RATE_LIMITER_ONE_PERMIT_PER_CALL, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_PERMITS_PER_EXECUTION_DOC)
                //header settings
                .define(prefix + STATIC_REQUEST_HEADER_NAMES, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, CONFIG_STATIC_REQUEST_HEADER_NAMES_DOC)
                .define(prefix + GENERATE_MISSING_CORRELATION_ID, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.MEDIUM, CONFIG_GENERATE_MISSING_CORRELATION_ID_DOC)
                .define(prefix + GENERATE_MISSING_REQUEST_ID, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.MEDIUM, CONFIG_GENERATE_MISSING_REQUEST_ID_DOC)
                .define(prefix + USER_AGENT_OVERRIDE, ConfigDef.Type.STRING, "http_client", ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_OVERRIDE_DOC)
                .define(prefix + USER_AGENT_CUSTOM_VALUES, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_CUSTOM_VALUES_DOC)

                //http response
                .define(prefix + HTTP_RESPONSE_MESSAGE_STATUS_LIMIT, ConfigDef.Type.INT, 1024, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT_DOC)
                .define(prefix + HTTP_RESPONSE_HEADERS_LIMIT, ConfigDef.Type.INT, 10_000, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT_DOC)
                .define(prefix + HTTP_RESPONSE_BODY_LIMIT, ConfigDef.Type.INT, 100_000, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_RESPONSE_BODY_LIMIT_DOC);
        String staticHeaderNames = settings.get(prefix + STATIC_REQUEST_HEADER_NAMES);
        if (staticHeaderNames != null && !staticHeaderNames.isBlank()) {
            List<String> staticHeaders = Arrays.asList(staticHeaderNames.split(","));
            for (String staticHeader : staticHeaders) {
                configDef = configDef.define(prefix + "enrich.request.static.header." + staticHeader, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_STATIC_REQUEST_HEADER_NAMES_DOC);
            }
        }
        return configDef1;
    }

    private ConfigDef appendHttpRequestMapperConfigDef(ConfigDef configDef, String httpRequestMapperName) {
        String prefix = "http.request.mapper." + httpRequestMapperName + ".";
        return configDef
                .define(prefix + REQUEST_MAPPER_DEFAULT_MODE, ConfigDef.Type.STRING, MapperMode.DIRECT.name(), ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_MODE_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, REQUEST_MAPPER_DEFAULT_URL_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_SPLIT_PATTERN, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_SPLIT_PATTERN_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_SPLIT_LIMIT, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_SPLIT_LIMIT_DOC);
    }


}
