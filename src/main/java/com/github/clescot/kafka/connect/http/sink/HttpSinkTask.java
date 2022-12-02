package com.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.github.clescot.kafka.connect.http.sink.client.HttpException;
import com.github.clescot.kafka.connect.http.sink.client.ahc.AHCHttpClientFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.Failsafe;
import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.clescot.kafka.connect.http.sink.client.HttpClient.*;


public class HttpSinkTask extends SinkTask {
    private final static Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    public static final String HEADER_X_CORRELATION_ID = "X-Correlation-ID";
    public static final String HEADER_X_REQUEST_ID = "X-Request-ID";

    private HttpClient httpClient;
    private Queue<HttpExchange> queue;
    private String queueName;

    private Map<String, List<String>> staticRequestHeaders;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private ErrantRecordReporter errantRecordReporter;
    private boolean generateMissingCorrelationId;
    private boolean generateMissingRequestId;
    private RateLimiter<HttpExchange> defaultRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, Duration.of(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ChronoUnit.MILLIS)).build();

    private Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();

    private Map<String, Pattern> httpSuccessCodesPatterns = Maps.newHashMap();
    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    /**
     * rate limiter configuration :
     * org.asynchttpclient.throttle.http.max.connections simultaneous max connections
     * org.asynchttpclient.throttle.http.rate.limit.per.second max calls per second
     * org.asynchttpclient.throttle.http.max.wait.ms max wait time when call quota is reached.
     * asynchttpclient configuration is starting by : 'org.asynchttpclient.'
     *
     * @param settings
     */
    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings cannot be null");
        try {
            errantRecordReporter = context.errantRecordReporter();
            if (errantRecordReporter == null) {
                LOGGER.warn("Dead Letter Queue (DLQ) is not enabled. it is recommended to configure a Dead Letter Queue for a better error handling.");
            }
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            errantRecordReporter = null;
        }

        this.httpSinkConnectorConfig = new HttpSinkConnectorConfig(HttpSinkConfigDefinition.config(), settings);

        this.queueName = httpSinkConnectorConfig.getQueueName();
        this.queue = QueueFactory.getQueue(queueName);
        this.staticRequestHeaders = httpSinkConnectorConfig.getStaticRequestHeaders();
        this.generateMissingRequestId = httpSinkConnectorConfig.isGenerateMissingRequestId();
        this.generateMissingCorrelationId = httpSinkConnectorConfig.isGenerateMissingCorrelationId();

        this.httpClient = new AHCHttpClientFactory().build(httpSinkConnectorConfig.originalsStrings());
        Integer defaultRetries = httpSinkConnectorConfig.getDefaultRetries();
        Long defaultRetryDelayInMs = httpSinkConnectorConfig.getDefaultRetryDelayInMs();
        Long defaultRetryMaxDelayInMs = httpSinkConnectorConfig.getDefaultRetryMaxDelayInMs();
        Double defaultRetryDelayFactor = httpSinkConnectorConfig.getDefaultRetryDelayFactor();
        Long defaultRetryJitterInMs = httpSinkConnectorConfig.getDefaultRetryJitterInMs();

        setDefaultRetryPolicy(
                defaultRetries,
                defaultRetryDelayInMs,
                defaultRetryMaxDelayInMs,
                defaultRetryDelayFactor,
                defaultRetryJitterInMs
        );
        setDefaultRateLimiter(httpSinkConnectorConfig.getDefaultRateLimiterPeriodInMs(),httpSinkConnectorConfig.getDefaultRateLimiterMaxExecutions());

        if (httpSinkConnectorConfig.isPublishToInMemoryQueue()) {
            Preconditions.checkArgument(QueueFactory.hasAConsumer(queueName, httpSinkConnectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs()), "'" + queueName + "' queue hasn't got any consumer, i.e no Source Connector has been configured to consume records published in this in memory queue. we stop the Sink Connector to prevent any OutofMemoryError.");
        }
    }



    @Override
    public void put(Collection<SinkRecord> records) {

        Preconditions.checkNotNull(records, "records collection to be processed is null");
        if (records.isEmpty()) {
            return;
        }
        Preconditions.checkNotNull(httpClient, "httpClient is null. 'start' method must be called once before put");

        for (SinkRecord sinkRecord : records) {
            try {
                // attempt to send record to data sink
                process(sinkRecord);
            } catch (Exception e) {
                if (errantRecordReporter != null) {
                    // Send errant record to error reporter
                    Future<Void> future = errantRecordReporter.report(sinkRecord, e);
                    // Optionally wait till the failure's been recorded in Kafka
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    // There's no error reporter, so fail
                    throw new ConnectException("Failed on record", e);
                }
            }
        }

    }

    private void process(SinkRecord sinkRecord) {
        if (sinkRecord.value() == null) {
            throw new ConnectException("sinkRecord Value is null :" + sinkRecord);
        }
        //build HttpRequest
        HttpRequest httpRequest = buildHttpRequest(sinkRecord);
        HttpRequest httpRequestWithStaticHeaders = addStaticHeaders(httpRequest);
        HttpRequest httpRequestWithTrackingHeaders = addTrackingHeaders(httpRequestWithStaticHeaders);
        //handle Request and Response
        HttpExchange httpExchange = callWithRetryPolicy(httpRequestWithTrackingHeaders, defaultRetryPolicy);
        LOGGER.debug("HTTP exchange :{}", httpExchange);

        //publish eventually to 'in memory' queue
        if (httpSinkConnectorConfig.isPublishToInMemoryQueue() && httpExchange != null) {
            LOGGER.debug("http exchange published to queue '{}':{}",queueName, httpExchange);
            queue.offer(httpExchange);
        } else {
            LOGGER.debug("http exchange NOT published to queue '{}':{}",queueName, httpExchange);
        }
    }

    private HttpExchange callWithRetryPolicy(HttpRequest httpRequest, Optional<RetryPolicy<HttpExchange>> retryPolicyForCall) {
        HttpExchange httpExchange = null;

        if (httpRequest != null) {
            AtomicInteger attempts = new AtomicInteger();
            try {
                attempts.addAndGet(ONE_HTTP_REQUEST);
                if (retryPolicyForCall.isPresent()) {
                    httpExchange = Failsafe.with(List.of(retryPolicyForCall.get()))
                            .get(() -> {
                                HttpExchange httpExchange1 = callWithThrottling(httpRequest, attempts);
                                return handleRetry(httpExchange1);
                            });
                } else {
                    return callWithThrottling(httpRequest, attempts);
                }
            } catch (Throwable throwable) {
                LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, throwable,
                        throwable.getMessage());
                return httpClient.buildHttpExchange(
                        httpRequest,
                        new HttpResponse(SERVER_ERROR_STATUS_CODE, String.valueOf(throwable.getMessage()), BLANK_RESPONSE_CONTENT),
                        Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID)),
                        attempts,
                        FAILURE);
            }
        }
        return httpExchange;
    }

    private HttpExchange handleRetry(HttpExchange httpExchange){
        boolean retry = retryNeeded(httpExchange.getHttpResponse());
        if(retry){
            throw new HttpException(httpExchange,"retry needed");
        }
        return httpExchange;
    }
    private Pattern getSuccessPattern(String customSuccessCodePattern) {
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
        String currentSuccessCodePattern = Optional.ofNullable(customSuccessCodePattern).orElse("^[1-4][0-9][0-9]$");

        if (this.httpSuccessCodesPatterns.get(currentSuccessCodePattern) == null) {
            //Pattern.compile should be reused for performance, but wsSuccessCode can change....
            Pattern httpSuccessPattern = Pattern.compile(currentSuccessCodePattern);
            httpSuccessCodesPatterns.put(currentSuccessCodePattern, httpSuccessPattern);
        }
        return httpSuccessCodesPatterns.get(currentSuccessCodePattern);
    }
    private boolean retryNeeded(HttpResponse httpResponse){
        Pattern successPattern = getSuccessPattern(null);
        return retryNeeded(httpResponse.getStatusCode(), successPattern);
    }

    private HttpExchange callWithThrottling(HttpRequest httpRequest, AtomicInteger attempts){
        try {
            this.defaultRateLimiter.acquirePermits(ONE_HTTP_REQUEST);
            LOGGER.debug("permits acquired");
            return httpClient.call(httpRequest,attempts);
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire execution permit from the rate limiter {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        }
    }

    private boolean retryNeeded(int responseStatusCode, Pattern pattern) throws HttpException{
        Matcher matcher = pattern.matcher("" + responseStatusCode);
        return !matcher.matches() && responseStatusCode >= 500;
    }


    private HttpRequest addTrackingHeaders(HttpRequest httpRequest) {
        if (httpRequest == null) {
            LOGGER.warn("sinkRecord has got a 'null' value");
            throw new ConnectException("sinkRecord has got a 'null' value");
        }
        Map<String, List<String>> headers = Optional.ofNullable(httpRequest.getHeaders()).orElse(Maps.newHashMap());

        //we generate an 'X-Request-ID' header if not present
        Optional<List<String>> requestId = Optional.ofNullable(httpRequest.getHeaders().get(HEADER_X_REQUEST_ID));
        if (requestId.isEmpty() && this.generateMissingRequestId) {
            requestId = Optional.of(Lists.newArrayList(UUID.randomUUID().toString()));
        }
        requestId.ifPresent(reqId -> headers.put(HEADER_X_REQUEST_ID, Lists.newArrayList(reqId)));

        //we generate an 'X-Correlation-ID' header if not present
        Optional<List<String>> correlationId = Optional.ofNullable(httpRequest.getHeaders().get(HEADER_X_CORRELATION_ID));
        if (correlationId.isEmpty() && this.generateMissingCorrelationId) {
            correlationId = Optional.of(Lists.newArrayList(UUID.randomUUID().toString()));
        }
        correlationId.ifPresent(corrId -> headers.put(HEADER_X_CORRELATION_ID, Lists.newArrayList(corrId)));

        return httpRequest;
    }

    protected HttpRequest buildHttpRequest(SinkRecord sinkRecord) {
        if (sinkRecord == null || sinkRecord.value() == null) {
            LOGGER.warn("sinkRecord has got a 'null' value");
            throw new ConnectException("sinkRecord has got a 'null' value");
        }
        HttpRequest httpRequest = null;
        Object value = sinkRecord.value();
        String stringValue = null;
        try {
            Class<?> valueClass = value.getClass();
            LOGGER.debug("valueClass is {}", valueClass.getName());
            if (Struct.class.isAssignableFrom(valueClass)) {
                Struct valueAsStruct = (Struct) value;

                valueAsStruct.validate();
                httpRequest = HttpRequest
                        .Builder
                        .anHttpRequest()
                        .withStruct(valueAsStruct)
                        .build();

            } else if ("[B".equals(valueClass.getName())) {
                //we assume the value is a byte array
                stringValue = new String((byte[]) value, Charsets.UTF_8);
            } else if (String.class.isAssignableFrom(valueClass)) {
                stringValue = (String) value;
            } else {
                LOGGER.warn("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
                throw new ConnectException("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
            }
            if (httpRequest == null) {
                httpRequest = parseHttpRequestAsJsonString(stringValue);
                LOGGER.debug("successful httpRequest parsing :{}", httpRequest);
            }
        } catch (ConnectException connectException) {
            LOGGER.warn("error in sinkRecord's structure : " + sinkRecord, connectException);
            if (errantRecordReporter != null) {
                errantRecordReporter.report(sinkRecord, connectException);
            } else {
                LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
                throw connectException;
            }

        }
        return httpRequest;
    }

    private HttpRequest parseHttpRequestAsJsonString(String value) throws ConnectException {
        HttpRequest httpRequest;
        try {
            httpRequest = OBJECT_MAPPER.readValue(value, HttpRequest.class);
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage(), e);
            throw new ConnectException(e);
        }
        return httpRequest;
    }

    private HttpRequest addStaticHeaders(HttpRequest httpRequest) {
        if (httpRequest != null) {
            this.staticRequestHeaders.forEach((key, value) -> httpRequest.getHeaders().put(key, value));
        } else {

        }
        return httpRequest;
    }


    @Override
    public void stop() {
        //Producer are stopped in connector stop
    }

    //for testing purpose
    protected void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    //for testing purpose
    protected Map<String, List<String>> getStaticRequestHeaders() {
        //we return a copy
        return Maps.newHashMap(staticRequestHeaders);
    }

    protected void setQueue(Queue<HttpExchange> queue) {
        this.queue = queue;
    }


    public void setDefaultRateLimiter(long periodInMs, long maxExecutions) {
        LOGGER.info("default rate limiter set with  {} executions every {} ms",maxExecutions,periodInMs);
        this.defaultRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
    }

    private void setDefaultRetryPolicy(Integer retries, Long retryDelayInMs, Long retryMaxDelayInMs, Double retryDelayFactor, Long retryJitterInMs) {
        this.defaultRetryPolicy = Optional.of(buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitterInMs));
    }

    private RetryPolicy<HttpExchange> buildRetryPolicy(Integer retries,
                                                       Long retryDelayInMs,
                                                       Long retryMaxDelayInMs,
                                                       Double retryDelayFactor,
                                                       Long retryJitterInMs) {
        return RetryPolicy.<HttpExchange>builder()
                //we retry only if the error comes from the WS server (server-side technical error)
                .handle(HttpException.class)
                .withBackoff(Duration.ofMillis(retryDelayInMs), Duration.ofMillis(retryMaxDelayInMs), retryDelayFactor)
                .withJitter(Duration.ofMillis(retryJitterInMs))
                .withMaxRetries(retries)
                .onRetry(listener -> LOGGER.warn("Retry ws call result:{}, failure:{}", listener.getLastResult(), listener.getLastException()))
                .onFailure(listener -> LOGGER.warn("ws call failed ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .onAbort(listener -> LOGGER.warn("ws call aborted ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .build();
    }

}
