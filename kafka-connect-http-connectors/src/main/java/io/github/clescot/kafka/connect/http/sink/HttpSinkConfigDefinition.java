package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;

public class HttpSinkConfigDefinition {


    //publish to in memory queue
    public static final String PUBLISH_TO_IN_MEMORY_QUEUE = "publish.to.in.memory.queue";
    public static final String PUBLISH_TO_IN_MEMORY_QUEUE_DOC = "when set to false, ignore HTTP responses, i.e does not publish responses in the in memory queue. No Source Connector is needed when set to false. When set to true, a Source Connector is needed to consume published Http exchanges in this in memory queue.";

    private static final long DEFAULT_WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS = 60000L;
    public static final String WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS = "wait.time.registration.queue.consumer.in.ms";
    public static final String WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC = "wait time defined with the '"+ WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS +"' parameter, for a queue consumer (Source Connector) registration. " +
            "We wait if the "+PUBLISH_TO_IN_MEMORY_QUEUE+" parameter is set to 'true', to avoid to publish to the queue without any consumer (OutOfMemoryError possible). default value is "+DEFAULT_WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS;

    private static final int DEFAULT_POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS = 2000;
    public static final String POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS = "poll.delay.registration.queue.consumer.in.ms";
    public static final String POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC = "poll delay, i.e, wait time before start polling a registered consumer defined with the '"+ POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS +"' parameter, " +
            "for a queue consumer (Source Connector) registration.if not set, default value is "+DEFAULT_POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS;
    private static final int DEFAULT_POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS = 500;
    public static final String POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS = "poll.interval.registration.queue.consumer.in.ms";
    public static final String POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC = "poll interval, i.e, time between every poll for a registered consumer defined with the '"+ POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS +"' parameter, " +
            "for a queue consumer (Source Connector) registration.if not set, default value is "+DEFAULT_POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS;

    //configuration
    public static final String CONFIGURATION_IDS ="config.ids";
    public static final String CONFIGURATION_IDS_DOC ="custom configurations id list. 'default' configuration is already registered.";

    public static final String DEFAULT_CONFIGURATION_PREFIX = "config.default.";

    //retry policy
    public static final String DEFAULT_RETRY_POLICY_PREFIX = "retry.policy.";

    //default values
    private static final int DEFAULT_RETRIES_VALUE = 1;
    private static final long DEFAULT_RETRY_DELAY_IN_MS_VALUE = 2000L;
    private static final long DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE = 20000L;
    private static final double DEFAULT_RETRY_DELAY_FACTOR_VALUE = 1.5d;
    private static final long DEFAULT_RETRY_JITTER_IN_MS_VALUE = 500;


    public static final String RETRIES = DEFAULT_RETRY_POLICY_PREFIX+"retries";
    public static final String CONFIG_DEFAULT_RETRIES = DEFAULT_CONFIGURATION_PREFIX + DEFAULT_RETRY_POLICY_PREFIX+ RETRIES;
    public static final String CONFIG_DEFAULT_RETRIES_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how many retries before an error is thrown";

    public static final String RETRY_DELAY_IN_MS = "retry.delay.in.ms";
    public static final String CONFIG_DEFAULT_RETRY_DELAY_IN_MS = DEFAULT_CONFIGURATION_PREFIX + DEFAULT_RETRY_POLICY_PREFIX + RETRY_DELAY_IN_MS;
    public static final String CONFIG_DEFAULT_RETRY_DELAY_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long wait initially before first retry";

    public static final String RETRY_MAX_DELAY_IN_MS = "retry.max.delay.in.ms";
    public static final String CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS = DEFAULT_CONFIGURATION_PREFIX + DEFAULT_RETRY_POLICY_PREFIX+RETRY_MAX_DELAY_IN_MS;
    public static final String CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long max wait before retry";

    public static final String RETRY_DELAY_FACTOR = "retry.delay.factor";
    public static final String CONFIG_DEFAULT_RETRY_DELAY_FACTOR = DEFAULT_CONFIGURATION_PREFIX + DEFAULT_RETRY_POLICY_PREFIX+RETRY_DELAY_FACTOR;
    public static final String CONFIG_DEFAULT_RETRY_DELAY_FACTOR_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define the factor to multiply the previous delay to define the current retry delay";

    public static final String RETRY_JITTER_IN_MS = "retry.jitter.in.ms";
    public static final String CONFIG_DEFAULT_RETRY_JITTER_IN_MS = DEFAULT_CONFIGURATION_PREFIX + DEFAULT_RETRY_POLICY_PREFIX+RETRY_JITTER_IN_MS;
    public static final String CONFIG_DEFAULT_RETRY_JITTER_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. " +
            "Define max entropy to add, to prevent many retry policies instances with the same parameters, to flood servers at the same time";

    //rate limiter
    public static final String DEFAULT_RATE_LIMITER_PREFIX = "rate.limiter.";
    public static final String RATE_LIMITER_PERIOD_IN_MS = DEFAULT_RATE_LIMITER_PREFIX+"period.in.ms";
    public static final String CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS = DEFAULT_CONFIGURATION_PREFIX + RATE_LIMITER_PERIOD_IN_MS;
    public static final String CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS_DOC = "period of time in milliseconds, during the max execution cannot be exceeded";

    public static final String RATE_LIMITER_MAX_EXECUTIONS = DEFAULT_RATE_LIMITER_PREFIX+"max.executions";
    public static final String CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS = DEFAULT_CONFIGURATION_PREFIX + RATE_LIMITER_MAX_EXECUTIONS;
    public static final String CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_DOC = "max executions in the period defined with the '"+ CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS +"' parameter";

    public static final String RATE_LIMITER_SCOPE = DEFAULT_RATE_LIMITER_PREFIX+"scope";
    public static final String CONFIG_DEFAULT_RATE_LIMITER_SCOPE = DEFAULT_CONFIGURATION_PREFIX + RATE_LIMITER_SCOPE;
    public static final String CONFIG_DEFAULT_RATE_LIMITER_SCOPE_DOC = "scope of the '"+ CONFIG_DEFAULT_RATE_LIMITER_SCOPE +"' parameter. can be either 'instance' (i.e a rate limiter per configuration in the connector instance),  or 'static' (a rate limiter per configuration shared with all connectors instances in the same Java Virtual Machine.";

    public static final long DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE = 1000L;
    public static final long DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE = 1L;
    public static final String DEFAULT_RATE_LIMITER_SCOPE_VALUE = "instance";

    //enrich HttpRequest
    public static final String ENRICH_REQUEST ="enrich.request.";
    public static final String STATIC_REQUEST_HEADER_PREFIX =ENRICH_REQUEST+"static.header.";
    public static final String STATIC_REQUEST_HEADER_NAMES =STATIC_REQUEST_HEADER_PREFIX+"names";
    public static final String CONFIG_STATIC_REQUEST_HEADER_NAMES =DEFAULT_CONFIGURATION_PREFIX + STATIC_REQUEST_HEADER_NAMES;
    public static final String CONFIG_STATIC_REQUEST_HEADER_NAMES_DOC = "list of static parameters names which will be added to all http requests. these parameter names need to be added with their values as parameters in complement of this list";


    public static final String GENERATE_MISSING_CORRELATION_ID = ENRICH_REQUEST+"generate.missing.correlation.id";
    public static final String CONFIG_GENERATE_MISSING_CORRELATION_ID = DEFAULT_CONFIGURATION_PREFIX + GENERATE_MISSING_CORRELATION_ID;
    public static final String CONFIG_GENERATE_MISSING_CORRELATION_ID_DOC = "if not present in the HttpRequest headers, generate an UUID bound to the 'X-Correlation-ID' name";

    public static final String GENERATE_MISSING_REQUEST_ID = ENRICH_REQUEST+"generate.missing.request.id";
    public static final String CONFIG_GENERATE_MISSING_REQUEST_ID = DEFAULT_CONFIGURATION_PREFIX + GENERATE_MISSING_REQUEST_ID;
    public static final String CONFIG_GENERATE_MISSING_REQUEST_ID_DOC = "if not present in the HttpRequest headers, generate an UUID bound to the 'X-Request-ID' name";

    //enrich httpExchange
    public static final String ENRICH_EXCHANGE ="enrich.exchange.";
    public static final String SUCCESS_RESPONSE_CODE_REGEX = ENRICH_EXCHANGE+"success.response.code.regex";
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

    private static final String DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX = "^5[0-9][0-9]$";

    public static final String CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX = "^[1-2][0-9][0-9]$";
    public static final String RETRY_RESPONSE_CODE_REGEX = DEFAULT_RETRY_POLICY_PREFIX+"response.code.regex";
    public static final String CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX = DEFAULT_CONFIGURATION_PREFIX + RETRY_RESPONSE_CODE_REGEX;
    public static final String DEFAULT_RETRY_RESPONSE_CODE_REGEX_DOC = "regex which define if a retry need to be triggered, based on the response status code. default is '"+ CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX +"'";


    //http client prefix
    public static final String HTTP_CLIENT_PREFIX = "httpclient.";
    public static final String OKHTTP_PREFIX = "okhttp.";
    public static final String AHC_PREFIX = "ahc.";

    public static final String HTTP_CLIENT_IMPLEMENTATION = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX + "implementation";
    public static final String HTTP_CLIENT_IMPLEMENTATION_DOC = "define which intalled library to use : either 'ahc', a.k.a async http client, or 'okhttp'. default is 'okhttp'.";

    public static final String OKHTTP_IMPLEMENTATION = "okhttp";
    public static final String AHC_IMPLEMENTATION = "ahc";


    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.keystore.path";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH_DOC = "file path of the custom key store.";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.keystore.password";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD_DOC = "password of the custom key store.";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.keystore.type";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE_DOC = "keystore type. can be 'jks' or 'pkcs12'.";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.keystore.algorithm";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM_DOC = "the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.";

    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.truststore.always.trust";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST_DOC = "trust store that always trust any certificate. this option remove any security on the transport layer. be careful when you activate this option ! you will have no guarantee that you don't contact any hacked server ! ";


    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.truststore.path";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH_DOC = "file path of the custom trust store.";

    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.truststore.password";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD_DOC = "password of the custom trusted store.";

    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.truststore.type";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE_DOC = "truststore type. can be 'jks' or 'pkcs12'.";

    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"ssl.truststore.algorithm";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM_DOC = "the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.";

    public static final String CONFIG_HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PREFIX +"async.fixed.thread.pool.size";
    public static final String CONFIG_HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE_DOC ="custom fixed thread pool size used to execute asynchronously http requests.";

    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE = HTTP_CLIENT_PREFIX+"authentication.basic.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE;
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

    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE = HTTP_CLIENT_PREFIX+"authentication.digest.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE =  DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE;
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

    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM = HTTP_CLIENT_PREFIX + "authentication.digest.secure.random.prng.algorithm";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM_DOC = "name of the Random Number Generator (RNG) algorithm used in the digest algorithm. cf https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#securerandom-number-generation-algorithms";

    //proxy
    public static final String HTTP_CLIENT_PROXY_HOSTNAME = HTTP_CLIENT_PREFIX + "proxy.hostname";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_HOSTNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_HOSTNAME;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_HOSTNAME_DOC = "hostname of the proxy host.";

    public static final String HTTP_CLIENT_PROXY_PORT = HTTP_CLIENT_PREFIX + "proxy.port";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_PORT = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_PORT;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_PORT_DOC = "hostname of the proxy host.";

    public static final String HTTP_CLIENT_PROXY_TYPE = HTTP_CLIENT_PREFIX + "proxy.type";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_TYPE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_TYPE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_TYPE_DOC = "type of proxy. can be either 'HTTP' (default), 'DIRECT' (i.e no proxy), or 'SOCKS'";

    //okhttp settings
    //cache
    public static final String OKHTTP_CACHE_ACTIVATE = OKHTTP_PREFIX+"cache.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CACHE_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE_DOC = "set to true to activate page cache (if cache hit, the server will not receive the request, and the response will comes from the cache). default is false.";

    public static final String OKHTTP_CACHE_MAX_SIZE = OKHTTP_PREFIX+"cache.max.size";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CACHE_MAX_SIZE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE_DOC = "max size of the page cache.";

    public static final String OKHTTP_CACHE_TYPE = OKHTTP_PREFIX+"cache.type";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_TYPE = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CACHE_TYPE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_TYPE_DOC = "persistance of the cache : either 'file'(default), or 'inmemory'.";

    public static final String OKHTTP_CACHE_DIRECTORY_PATH = OKHTTP_PREFIX+"cache.directory.path";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CACHE_DIRECTORY_PATH;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH_DOC = "file system path of the cache directory.";


    //connection
    public static final String OKHTTP_CALL_TIMEOUT = OKHTTP_PREFIX+"call.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CALL_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT_DOC = "default timeout in milliseconds for complete call . A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_READ_TIMEOUT = OKHTTP_PREFIX+"read.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_READ_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT_DOC = "Sets the default read timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_CONNECT_TIMEOUT = OKHTTP_PREFIX+"connect.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CONNECT_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT_DOC = "Sets the default connect timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_WRITE_TIMEOUT = OKHTTP_PREFIX+"write.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_WRITE_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT_DOC = "Sets the default write timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION = OKHTTP_PREFIX+"ssl.skip.hostname.verification";
    public static final String CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION;
    public static final String CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION_DOC = "if set to 'true', skip hostname verification. Not set by default.";


    //protocols to use, in order of preference,divided by a comma.supported protocols in okhttp: HTTP_1_1,HTTP_2,H2_PRIOR_KNOWLEDGE,QUIC
    public static final String OKHTTP_PROTOCOLS = OKHTTP_PREFIX+"protocols";
    public static final String CONFIG_DEFAULT_OKHTTP_PROTOCOLS =DEFAULT_CONFIGURATION_PREFIX+ OKHTTP_PROTOCOLS;
    public static final String CONFIG_DEFAULT_OKHTTP_PROTOCOLS_DOC ="the protocols to use, in order of preference. If the list contains Protocol.H2_PRIOR_KNOWLEDGE then that must be the only protocol and HTTPS URLs will not be supported. Otherwise the list must contain Protocol.HTTP_1_1. The list must not contain null or Protocol.HTTP_1_0.";

    public static final String OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION = OKHTTP_PREFIX+"connection.pool.keep.alive.duration";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION_DOC = "Time in milliseconds to keep the connection alive in the pool before closing it. Default is 0 (no connection pool).";

    public static final String OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS = OKHTTP_PREFIX+"connection.pool.max.idle.connections";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS = DEFAULT_CONFIGURATION_PREFIX+OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DOC ="amount of connections to keep idle, to avoid the connection creation time when needed. Default is 0 (no connection pool)";

    public static final String OKHTTP_FOLLOW_REDIRECT = OKHTTP_PREFIX + "follow.redirect";
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_FOLLOW_REDIRECT;
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT_DOC = "does the http client need to follow a redirect response from the server. default to true.";

    public static final String OKHTTP_FOLLOW_SSL_REDIRECT = OKHTTP_PREFIX + "follow.ssl.redirect";
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_FOLLOW_SSL_REDIRECT;
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT_DOC = "does the http client need to follow an SSL redirect response from the server. default to true.";


    private HttpSinkConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                //http client implementation settings
                .define(HTTP_CLIENT_IMPLEMENTATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HTTP_CLIENT_IMPLEMENTATION_DOC)
                //retry settings
                .define(CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX_DOC)
                .define(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, DEFAULT_RETRY_RESPONSE_CODE_REGEX_DOC)
                .define(CONFIG_DEFAULT_RETRIES, ConfigDef.Type.INT, DEFAULT_RETRIES_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRIES_DOC)
                .define(CONFIG_DEFAULT_RETRY_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_DELAY_IN_MS_DOC)
                .define(CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS_DOC)
                .define(CONFIG_DEFAULT_RETRY_DELAY_FACTOR, ConfigDef.Type.DOUBLE, DEFAULT_RETRY_DELAY_FACTOR_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_DELAY_FACTOR_DOC)
                .define(CONFIG_DEFAULT_RETRY_JITTER_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_JITTER_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_JITTER_IN_MS_DOC)
                //rate limiting settings
                .define(CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS, ConfigDef.Type.LONG, HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS_DOC)
                .define(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, ConfigDef.Type.LONG, HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_DOC)
                .define(CONFIG_DEFAULT_RATE_LIMITER_SCOPE, ConfigDef.Type.STRING, HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_SCOPE_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_SCOPE_DOC)
                //header settings
                .define(CONFIG_STATIC_REQUEST_HEADER_NAMES, ConfigDef.Type.LIST,  Collections.emptyList(), ConfigDef.Importance.MEDIUM, CONFIG_STATIC_REQUEST_HEADER_NAMES_DOC)
                .define(CONFIG_GENERATE_MISSING_CORRELATION_ID, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, CONFIG_GENERATE_MISSING_CORRELATION_ID_DOC)
                .define(CONFIG_GENERATE_MISSING_REQUEST_ID, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, CONFIG_GENERATE_MISSING_REQUEST_ID_DOC)
                //in memory queue settings
                .define(PUBLISH_TO_IN_MEMORY_QUEUE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, PUBLISH_TO_IN_MEMORY_QUEUE_DOC)
                .define(ConfigConstants.QUEUE_NAME, ConfigDef.Type.STRING, null,ConfigDef.Importance.MEDIUM, ConfigConstants.QUEUE_NAME_DOC)
                .define(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS, ConfigDef.Type.LONG, DEFAULT_WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS, ConfigDef.Importance.LOW, WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC)
                .define(POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS, ConfigDef.Type.INT, DEFAULT_POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS, ConfigDef.Importance.LOW, POLL_DELAY_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC)
                .define(POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS, ConfigDef.Type.INT, DEFAULT_POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS, ConfigDef.Importance.LOW, POLL_INTERVAL_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC)
                //SSL settings
                .define(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE_DOC)
                .define(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM_DOC)
                //authentication
                //basic
                .define(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE_DOC)
                .define(CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_BASIC_USERNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_BASIC_USER_DOC)
                .define(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD_DOC)
                .define(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET, ConfigDef.Type.STRING, "ISO-8859-1", ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET_DOC)
                //digest
                .define(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE_DOC)
                .define(CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_DIGEST_USERNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_DIGEST_USER_DOC)
                .define(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD_DOC)
                .define(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET, ConfigDef.Type.STRING, "US-ASCII", ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET_DOC)
                .define(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM, ConfigDef.Type.STRING, "SHA1PRNG", ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM_DOC)
                //async settings
                .define(CONFIG_HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE, ConfigDef.Type.INT, null, ConfigDef.Importance.MEDIUM, CONFIG_HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE_DOC)
                //custom configurations
                .define(CONFIGURATION_IDS,ConfigDef.Type.LIST, Lists.newArrayList(),ConfigDef.Importance.LOW, CONFIGURATION_IDS_DOC)

                //'okhttp' settings
                //cache
                .define(CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE,ConfigDef.Type.BOOLEAN,false, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE,ConfigDef.Type.LONG,0, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_CACHE_TYPE,ConfigDef.Type.STRING,"file", ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_CACHE_TYPE_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH,ConfigDef.Type.STRING,null, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH_DOC)

                //connection
                .define(CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT,ConfigDef.Type.INT,0, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT,ConfigDef.Type.INT,0, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT,ConfigDef.Type.INT,0, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT,ConfigDef.Type.INT,0, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_PROTOCOLS,ConfigDef.Type.STRING,null, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_PROTOCOLS_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION,ConfigDef.Type.BOOLEAN,false, ConfigDef.Importance.LOW,CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION_DOC)
                //connection pool
                .define(CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS,ConfigDef.Type.INT, 0,ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION,ConfigDef.Type.LONG, 0,ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION_DOC)

                //follow redirect
                .define(CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT,ConfigDef.Type.BOOLEAN, true,ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT_DOC)
                .define(CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT,ConfigDef.Type.BOOLEAN, true,ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT_DOC)
                ;
    }
}
