package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.ConfigConstants;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;

public class HttpSinkConfigDefinition {

    public static final String STATIC_REQUEST_HEADER_NAMES = "static.request.header.names";
    public static final String STATIC_REQUEST_HEADER_NAMES_DOC = "list of static parameters names which will be added to all http requests. these parameter names need to be added with their values as parameters in complement of this list";
    public static final String PUBLISH_TO_IN_MEMORY_QUEUE = "publish.to.in.memory.queue";
    public static final String PUBLISH_TO_IN_MEMORY_QUEUE_DOC = "when set to false, ignore HTTP responses, i.e does not publish responses in the in memory queue. No Source Connector is needed when set to false. When set to true, a Source Connector is needed to consume published Http exchanges in this in memory queue.";
    public static final String DEFAULT_RETRIES = "default.retries";
    public static final String DEFAULT_RETRIES_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how many retries before an error is thrown";
    public static final String DEFAULT_RETRY_DELAY_IN_MS = "default.retry.delay.in.ms";
    public static final String DEFAULT_RETRY_DELAY_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long wait initially before first retry";
    public static final String DEFAULT_RETRY_MAX_DELAY_IN_MS = "default.retry.max.delay.in.ms";
    public static final String DEFAULT_RETRY_MAX_DELAY_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define how long max wait before retry";
    public static final String DEFAULT_RETRY_DELAY_FACTOR = "default.retry.delay.factor";
    public static final String DEFAULT_RETRY_DELAY_FACTOR_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define the factor to multiply the previous delay to define the current retry delay";
    public static final String DEFAULT_RETRY_JITTER_IN_MS = "default.retry.jitter.in.ms";
    public static final String DEFAULT_RETRY_JITTER_IN_MS_DOC = "if set with other default retry parameters, permit to define a default retry policy, which can be overriden in the httpRequest object. Define max entropy to add, to prevent many retry policies instances with the same parameters, to flood servers at the same time";

    public static final String DEFAULT_RATE_LIMITER_PERIOD_IN_MS = "default.rate.limiter.period.in.ms";
    public static final String DEFAULT_RATE_LIMITER_PERIOD_IN_MS_DOC = "period of time in milliseconds, during the max execution cannot be exceeded";

    public static final String DEFAULT_RATE_LIMITER_MAX_EXECUTIONS = "default.rate.limiter.max.executions";
    public static final String DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_DOC = "max execution in the period defined with the '"+DEFAULT_RATE_LIMITER_PERIOD_IN_MS+"' parameter";

    public static final String WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS = "wait.time.registration.queue.consumer.in.ms";
    public static final String WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC = "wait time defined with the '"+DEFAULT_RATE_LIMITER_PERIOD_IN_MS+"' parameter, for a queue consumer (Source Connector) registration. We wait if the "+PUBLISH_TO_IN_MEMORY_QUEUE+" parameter is set to 'true', to avoid to publish to the queue without any consumer (OutOfMemoryError possible)";


    public static final String GENERATE_MISSING_CORRELATION_ID = "generate.missing.correlation.id";
    public static final String GENERATE_MISSING_CORRELATION_ID_DOC = "if not present in the HttpRequest headers, generate an UUID bound to the 'X-Correlation-ID' name";
    public static final String GENERATE_MISSING_REQUEST_ID = "generate.missing.request.id";
    public static final String GENERATE_MISSING_REQUEST_ID_DOC = "if not present in the HttpRequest headers, generate an UUID bound to the 'X-Request-ID' name";
    public static final long DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE = 1000L;
    public static final long DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE = 1L;
    private static final int DEFAULT_RETRIES_VALUE = 1;
    private static final long DEFAULT_RETRY_DELAY_IN_MS_VALUE = 2000L;
    private static final long DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE = 20000L;
    private static final double DEFAULT_RETRY_DELAY_FACTOR_VALUE = 1.5d;
    private static final long DEFAULT_RETRY_JITTER_IN_MS_VALUE = 500;


    public static final String HTTPCLIENT_IMPLEMENTATION = "httpclient.implementation";
    public static final String HTTPCLIENT_IMPLEMENTATION_DOC = "define which intalled library to use : either 'ahc', a.k.a async http client, or 'okhttp' ";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_PATH = "httpclient.ssl.truststore.path";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_PATH_DOC = "file path of the custom trusted store";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD = "httpclient.ssl.truststore.password";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD_DOC = "password of the custom trusted store";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_TYPE = "httpclient.ssl.truststore.type";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_TYPE_DOC = "truststore type. can be 'jks' or 'pkcs12'";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM = "httpclient.ssl.truststore.algorithm";
    public static final String HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM_DOC = "httpclient.ssl.truststore.algorithm";
    public static final String DEFAULT_SUCCESS_RESPONSE_CODE_REGEX = "default.success.response.code.regex";
    public static final String DEFAULT_SUCCESS_RESPONSE_CODE_REGEX_DOC = "default regex which decide if the request is a success or not, based on the response status code";
    private static final String DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX = "^[1-2][0-9][0-9]$";
    public static final String DEFAULT_RETRY_RESPONSE_CODE_REGEX = "default.retry.response.code.regex";
    public static final String DEFAULT_RETRY_RESPONSE_CODE_REGEX_DOC = "regex which define if a retry need to be triggered, based on the response status code";
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



    private HttpSinkConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstants.QUEUE_NAME, ConfigDef.Type.STRING, null,ConfigDef.Importance.MEDIUM, ConfigConstants.QUEUE_NAME_DOC)
                .define(STATIC_REQUEST_HEADER_NAMES, ConfigDef.Type.LIST,  Collections.emptyList(), ConfigDef.Importance.MEDIUM, STATIC_REQUEST_HEADER_NAMES_DOC)
                .define(PUBLISH_TO_IN_MEMORY_QUEUE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, PUBLISH_TO_IN_MEMORY_QUEUE_DOC)
                .define(DEFAULT_SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, DEFAULT_SUCCESS_RESPONSE_CODE_REGEX_DOC)
                .define(DEFAULT_RETRY_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, DEFAULT_RETRY_RESPONSE_CODE_REGEX_DOC)
                .define(DEFAULT_RETRIES, ConfigDef.Type.INT, DEFAULT_RETRIES_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_RETRIES_DOC)
                .define(DEFAULT_RETRY_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_RETRY_DELAY_IN_MS_DOC)
                .define(DEFAULT_RETRY_MAX_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_RETRY_MAX_DELAY_IN_MS_DOC)
                .define(DEFAULT_RETRY_DELAY_FACTOR, ConfigDef.Type.DOUBLE, DEFAULT_RETRY_DELAY_FACTOR_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_RETRY_DELAY_FACTOR_DOC)
                .define(DEFAULT_RETRY_JITTER_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_JITTER_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_RETRY_JITTER_IN_MS_DOC)
                .define(DEFAULT_RATE_LIMITER_PERIOD_IN_MS, ConfigDef.Type.LONG, HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_RATE_LIMITER_PERIOD_IN_MS_DOC)
                .define(DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, ConfigDef.Type.LONG, HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, ConfigDef.Importance.MEDIUM, DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_DOC)
                .define(GENERATE_MISSING_CORRELATION_ID, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, GENERATE_MISSING_CORRELATION_ID_DOC)
                .define(GENERATE_MISSING_REQUEST_ID, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, GENERATE_MISSING_REQUEST_ID_DOC)
                .define(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS, ConfigDef.Type.LONG, 60000L, ConfigDef.Importance.LOW, WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS_DOC)
                .define(HTTPCLIENT_IMPLEMENTATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HTTPCLIENT_IMPLEMENTATION_DOC)
                .define(HTTPCLIENT_SSL_TRUSTSTORE_PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HTTPCLIENT_SSL_TRUSTSTORE_PATH_DOC)
                .define(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(HTTPCLIENT_SSL_TRUSTSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HTTPCLIENT_SSL_TRUSTSTORE_TYPE_DOC)
                .define(HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM_DOC)
                ;
    }
}
