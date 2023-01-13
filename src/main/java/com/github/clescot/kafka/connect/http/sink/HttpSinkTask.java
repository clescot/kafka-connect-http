package com.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.clescot.kafka.connect.http.*;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import com.github.clescot.kafka.connect.http.sink.client.HttpException;
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

import java.lang.reflect.InvocationTargetException;
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
    private Queue<KafkaRecord> queue;
    private String queueName;

    private Map<String, List<String>> staticRequestHeaders;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private ErrantRecordReporter errantRecordReporter;
    private boolean generateMissingCorrelationId;
    private boolean generateMissingRequestId;
    private RateLimiter<HttpExchange> defaultRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, Duration.of(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ChronoUnit.MILLIS)).build();

    private Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();

    private Map<String, Pattern> patternMap = Maps.newHashMap();
    private String defaultSuccessResponseCodeRegex;
    private String defaultRetryResponseCodeRegex;

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    /**
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
        this.defaultSuccessResponseCodeRegex = httpSinkConnectorConfig.getDefaultSuccessResponseCodeRegex();
        this.defaultRetryResponseCodeRegex = httpSinkConnectorConfig.getDefaultRetryResponseCodeRegex();
        Class<HttpClientFactory> httpClientFactoryClass;
        HttpClientFactory httpClientFactory;
        try {
            httpClientFactoryClass = (Class<HttpClientFactory>) Class.forName(httpSinkConnectorConfig.getHttpClientFactoryClass());
            httpClientFactory = httpClientFactoryClass.getDeclaredConstructor().newInstance();
            LOGGER.debug("using HttpClientFactory implementation: {}",httpClientFactory.getClass().getName());
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        this.httpClient = httpClientFactory.build(httpSinkConnectorConfig.originalsStrings());
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
        setDefaultRateLimiter(httpSinkConnectorConfig.getDefaultRateLimiterPeriodInMs(), httpSinkConnectorConfig.getDefaultRateLimiterMaxExecutions());

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
        HttpExchange httpExchange = callWithRetryPolicy(sinkRecord, httpRequestWithTrackingHeaders, defaultRetryPolicy);
        LOGGER.debug("HTTP exchange :{}", httpExchange);
    }

    private HttpExchange callWithRetryPolicy(SinkRecord sinkRecord, HttpRequest httpRequest, Optional<RetryPolicy<HttpExchange>> retryPolicyForCall) {
        HttpExchange httpExchange = null;

        if (httpRequest != null) {
            AtomicInteger attempts = new AtomicInteger();
            try {
                attempts.addAndGet(ONE_HTTP_REQUEST);
                if (retryPolicyForCall.isPresent()) {
                    httpExchange = Failsafe.with(List.of(retryPolicyForCall.get()))
                            .get(() -> {
                                HttpExchange httpExchange1 = callAndPublish(sinkRecord, httpRequest, attempts);
                                return handleRetry(httpExchange1);
                            });
                } else {
                    return callAndPublish(sinkRecord, httpRequest, attempts);
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

    private HttpExchange handleRetry(HttpExchange httpExchange) {
        //we don't retry success HTTP Exchange
        boolean responseCodeImpliesRetry = retryNeeded(httpExchange.getHttpResponse());
        LOGGER.debug("httpExchange success :'{}'", httpExchange.isSuccess());
        LOGGER.debug("response code('{}') implies retry:'{}'", httpExchange.getHttpResponse().getStatusCode(), "" + responseCodeImpliesRetry);
        if (!httpExchange.isSuccess()
                && responseCodeImpliesRetry) {
            throw new HttpException(httpExchange, "retry needed");
        }
        return httpExchange;
    }

    private Pattern getPattern(String pattern) {

        if (this.patternMap.get(pattern) == null) {
            //Pattern.compile should be reused for performance, but wsSuccessCode can change....
            Pattern httpSuccessPattern = Pattern.compile(pattern);
            patternMap.put(pattern, httpSuccessPattern);
        }
        return patternMap.get(pattern);
    }

    protected boolean retryNeeded(HttpResponse httpResponse) {
        //TODO add specific pattern per site
        Pattern retryPattern = getPattern(this.defaultRetryResponseCodeRegex);
        Matcher matcher = retryPattern.matcher("" + httpResponse.getStatusCode());
        return matcher.matches();
    }

    private HttpExchange callWithThrottling(HttpRequest httpRequest, AtomicInteger attempts) {
        try {
            this.defaultRateLimiter.acquirePermits(ONE_HTTP_REQUEST);
            LOGGER.debug("permits acquired request:'{}'",httpRequest);
            return httpClient.call(httpRequest, attempts);
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire execution permit from the rate limiter {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        }
    }


    private HttpExchange callAndPublish(SinkRecord sinkRecord, HttpRequest httpRequest, AtomicInteger attempts) {
        HttpExchange httpExchange = callWithThrottling(httpRequest, attempts);
        //TODO add specific pattern per site
        boolean success = isSuccess(httpExchange);
        httpExchange.setSuccess(success);
        //publish eventually to 'in memory' queue
        if (httpSinkConnectorConfig.isPublishToInMemoryQueue()) {
            LOGGER.debug("http exchange published to queue '{}':{}", queueName, httpExchange);
            queue.offer(new KafkaRecord(sinkRecord.headers(), sinkRecord.keySchema(), sinkRecord.key(), httpExchange));
        } else {
            LOGGER.debug("http exchange NOT published to queue '{}':{}", queueName, httpExchange);
        }
        return httpExchange;
    }

    protected boolean isSuccess(HttpExchange httpExchange) {
        Pattern pattern = getPattern(this.defaultSuccessResponseCodeRegex);
        boolean success = pattern.matcher(httpExchange.getHttpResponse().getStatusCode() + "").matches();
        return success;
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
                LOGGER.debug("Struct is {}", valueAsStruct);
                valueAsStruct.validate();
                httpRequest = HttpRequest
                        .Builder
                        .anHttpRequest()
                        .withStruct(valueAsStruct)
                        .build();
                LOGGER.debug("httpRequest : {}", httpRequest);
            } else if ("[B".equals(valueClass.getName())) {
                //we assume the value is a byte array
                stringValue = new String((byte[]) value, Charsets.UTF_8);
                LOGGER.debug("byte[] is {}", stringValue);
            } else if (String.class.isAssignableFrom(valueClass)) {
                stringValue = (String) value;
                LOGGER.debug("String is {}", stringValue);
            } else {
                LOGGER.warn("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
                throw new ConnectException("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
            }
            if (httpRequest == null) {
                LOGGER.debug("stringValue :{}", stringValue);
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
        Preconditions.checkNotNull(httpRequest,"httpRequest is null");
        this.staticRequestHeaders.forEach((key, value) -> httpRequest.getHeaders().put(key, value));
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

    protected void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }


    public void setDefaultRateLimiter(long periodInMs, long maxExecutions) {
        LOGGER.info("default rate limiter set with  {} executions every {} ms", maxExecutions, periodInMs);
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
