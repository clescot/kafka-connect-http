package io.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.Failsafe;
import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.sink.client.HttpClient;
import io.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.sink.client.HttpException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class HttpSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    public static final String HEADER_X_CORRELATION_ID = "X-Correlation-ID";
    public static final String HEADER_X_REQUEST_ID = "X-Request-ID";
    public static final String SINK_RECORD_HAS_GOT_A_NULL_VALUE = "sinkRecord has got a 'null' value";



    private HttpClient httpClient;
    private Queue<KafkaRecord> queue;
    private String queueName;

    private Map<String, List<String>> staticRequestHeaders;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private ErrantRecordReporter errantRecordReporter;
    private boolean generateMissingCorrelationId;
    private boolean generateMissingRequestId;

    private final Map<String, Pattern> patternMap = Maps.newHashMap();
    private String defaultSuccessResponseCodeRegex;
    private String defaultRetryResponseCodeRegex;
    private CopyOnWriteArrayList<Configuration> customConfigurations;
    private static ExecutorService executor;

    @Override
    public String version() {
        return VersionUtils.version(this.getClass());
    }

    /**
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

        Integer customFixedThreadPoolSize = httpSinkConnectorConfig.getCustomFixedThreadpoolSize();
        if(customFixedThreadPoolSize!=null &&executor==null){
            executor = Executors.newFixedThreadPool(customFixedThreadPoolSize);
        }

        Class<HttpClientFactory> httpClientFactoryClass;
        HttpClientFactory httpClientFactory;
        try {
            httpClientFactoryClass = (Class<HttpClientFactory>) Class.forName(httpSinkConnectorConfig.getHttpClientFactoryClass());
            httpClientFactory = httpClientFactoryClass.getDeclaredConstructor().newInstance();
            LOGGER.debug("using HttpClientFactory implementation: {}", httpClientFactory.getClass().getName());
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        this.httpClient = httpClientFactory.build(httpSinkConnectorConfig.originalsStrings(),executor);

        customConfigurations = buildCustomConfigurations(httpSinkConnectorConfig);


        if (httpSinkConnectorConfig.isPublishToInMemoryQueue()) {
            Preconditions.checkArgument(QueueFactory.hasAConsumer(
                    queueName,
                    httpSinkConnectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs()
                    , httpSinkConnectorConfig.getPollDelayRegistrationOfQueueConsumerInMs(),
                    httpSinkConnectorConfig.getPollIntervalRegistrationOfQueueConsumerInMs()
            ), "timeout : '" + httpSinkConnectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs() +
                    "'ms timeout reached :" + queueName + "' queue hasn't got any consumer, " +
                    "i.e no Source Connector has been configured to consume records published in this in memory queue. " +
                    "we stop the Sink Connector to prevent any OutofMemoryError.");
        }
    }


    private CopyOnWriteArrayList<Configuration> buildCustomConfigurations(HttpSinkConnectorConfig httpSinkConnectorConfig){
        CopyOnWriteArrayList<Configuration> configurations = Lists.newCopyOnWriteArrayList();
        for (String configId: httpSinkConnectorConfig.getConfigurationIds()) {
           Configuration configuration = new Configuration(configId,httpSinkConnectorConfig);
            configurations.add(configuration);
        }
        return configurations;
    }


    @Override
    public void put(Collection<SinkRecord> records) {

        Preconditions.checkNotNull(records, "records collection to be processed is null");
        if (records.isEmpty()) {
            return;
        }
        Preconditions.checkNotNull(httpClient, "httpClient is null. 'start' method must be called once before put");

        //we submit futures to the pool
        Stream<SinkRecord> recordStream = records.stream();
        Stream<CompletableFuture<HttpExchange>> completableFutureStream;
        completableFutureStream= recordStream.map(this::process);

        List<CompletableFuture<HttpExchange>> completableFutures = completableFutureStream.collect(Collectors.toList());
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());


    }

    private CompletableFuture<HttpExchange> process(SinkRecord sinkRecord) {
        try {
            if (sinkRecord.value() == null) {
                throw new ConnectException("sinkRecord Value is null :" + sinkRecord);
            }
            //build HttpRequest
            HttpRequest httpRequest = buildHttpRequest(sinkRecord);
            HttpRequest httpRequestWithStaticHeaders = addStaticHeaders(httpRequest);
            HttpRequest httpRequestWithTrackingHeaders = addTrackingHeaders(httpRequestWithStaticHeaders);

            //TODO get configuration from HttpRequest
            Configuration defaultConfiguration = new Configuration("default",httpSinkConnectorConfig);
            Configuration foundConfiguration = customConfigurations.stream().filter(config -> config.matches(httpRequest)).findFirst().orElse(defaultConfiguration);

            //handle Request and Response
            return callWithRetryPolicy(sinkRecord, httpRequestWithTrackingHeaders, foundConfiguration.getRetryPolicy(),foundConfiguration).thenApply(
                    myHttpExchange -> {
                        LOGGER.debug("HTTP exchange :{}", myHttpExchange);
                        return myHttpExchange;
                    }
            );
        } catch (Exception e) {
            if (errantRecordReporter != null) {
                // Send errant record to error reporter
                Future<Void> future = errantRecordReporter.report(sinkRecord, e);
                // Optionally wait till the failure's been recorded in Kafka
                try {
                    future.get();
                    return CompletableFuture.failedFuture(e);
                } catch (InterruptedException | ExecutionException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                // There's no error reporter, so fail
                throw new ConnectException("Failed on record", e);
            }
        }

    }

    private CompletableFuture<HttpExchange> callWithRetryPolicy(SinkRecord sinkRecord, HttpRequest httpRequest, Optional<RetryPolicy<HttpExchange>> retryPolicyForCall,Configuration configuration) {

        if (httpRequest != null) {
            AtomicInteger attempts = new AtomicInteger();
            try {
                attempts.addAndGet(HttpClient.ONE_HTTP_REQUEST);
                if (retryPolicyForCall.isPresent()) {
                    RetryPolicy<HttpExchange> retryPolicy = retryPolicyForCall.get();
                    CompletableFuture<HttpExchange> httpExchangeFuture = callAndPublish(sinkRecord, httpRequest, attempts,configuration)
                            .thenApply(this::handleRetry);
                    return Failsafe.with(List.of(retryPolicy))
                            .getStageAsync(()->httpExchangeFuture);
                } else {
                    return callAndPublish(sinkRecord, httpRequest, attempts,configuration);
                }
            } catch (Throwable throwable) {
                LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, throwable,
                        throwable.getMessage());
                return CompletableFuture.supplyAsync(() -> httpClient.buildHttpExchange(
                        httpRequest,
                        new HttpResponse(HttpClient.SERVER_ERROR_STATUS_CODE, String.valueOf(throwable.getMessage())),
                        Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                        attempts,
                        HttpClient.FAILURE));
            }
        }else{
            throw new IllegalArgumentException("httpRequest is null");
        }
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

    private Pattern getRetryPattern(String pattern) {

        if (this.patternMap.get(pattern) == null) {
            //Pattern.compile should be reused for performance, but wsSuccessCode can change....
            Pattern httpSuccessPattern = Pattern.compile(pattern);
            patternMap.put(pattern, httpSuccessPattern);
        }
        return patternMap.get(pattern);
    }

    protected boolean retryNeeded(HttpResponse httpResponse) {
        //TODO add specific retry pattern per site
        Pattern retryPattern = getRetryPattern(this.defaultRetryResponseCodeRegex);
        Matcher matcher = retryPattern.matcher("" + httpResponse.getStatusCode());
        return matcher.matches();
    }

    private CompletableFuture<HttpExchange> callWithThrottling(HttpRequest httpRequest, AtomicInteger attempts,Configuration configuration) {
        try {
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getRateLimiter();
            if(rateLimiter.isPresent()) {
                rateLimiter.get().acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                LOGGER.debug("permits acquired request:'{}'", httpRequest);
            }
            return httpClient.call(httpRequest, attempts);
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire execution permit from the rate limiter {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        }
    }


    private CompletableFuture<HttpExchange> callAndPublish(SinkRecord sinkRecord, HttpRequest httpRequest, AtomicInteger attempts,Configuration configuration) {
        return callWithThrottling(httpRequest, attempts,configuration)
                .thenApply(myHttpExchange -> {
                    //TODO add specific pattern per site
                    boolean success = isSuccess(myHttpExchange);
                    myHttpExchange.setSuccess(success);
                    //publish eventually to 'in memory' queue
                    if (httpSinkConnectorConfig.isPublishToInMemoryQueue()) {
                        LOGGER.debug("http exchange published to queue '{}':{}", queueName, myHttpExchange);
                        queue.offer(new KafkaRecord(sinkRecord.headers(), sinkRecord.keySchema(), sinkRecord.key(), myHttpExchange));
                    } else {
                        LOGGER.debug("http exchange NOT published to queue '{}':{}", queueName, myHttpExchange);
                    }
                    return myHttpExchange;
                });

    }

    protected boolean isSuccess(HttpExchange httpExchange) {
        Pattern pattern = getRetryPattern(this.defaultSuccessResponseCodeRegex);
        return pattern.matcher(httpExchange.getHttpResponse().getStatusCode() + "").matches();
    }


    protected HttpRequest addTrackingHeaders(HttpRequest httpRequest) {
        if (httpRequest == null) {
            LOGGER.warn(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
            throw new ConnectException(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
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
            LOGGER.warn(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
            throw new ConnectException(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
        }
        HttpRequest httpRequest = null;
        Object value = sinkRecord.value();
        String stringValue = null;
        try {
            Class<?> valueClass = value.getClass();
            LOGGER.debug("valueClass is '{}'", valueClass.getName());
            LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
            if (Struct.class.isAssignableFrom(valueClass)) {
                Struct valueAsStruct = (Struct) value;
                LOGGER.debug("Struct is {}", valueAsStruct);
                valueAsStruct.validate();
                Schema schema = valueAsStruct.schema();
                String schemaTypeName = schema.type().getName();
                LOGGER.debug("schema type name referenced in Struct is '{}'", schemaTypeName);
                Integer version = schema.version();
                LOGGER.debug("schema version referenced in Struct is '{}'", version);

                httpRequest = HttpRequestAsStruct
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
            Object sinkValue = sinkRecord.value();

            if (sinkValue != null) {
                LOGGER.error("sink value class is '{}'", sinkValue.getClass().getName());
            }

            LOGGER.error("error in sinkRecord's structure : " + sinkRecord, connectException);
            if (errantRecordReporter != null) {
                errantRecordReporter.report(sinkRecord, connectException);
            } else {
                LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            }
            throw connectException;

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

    protected HttpRequest addStaticHeaders(HttpRequest httpRequest) {
        Preconditions.checkNotNull(httpRequest, "httpRequest is null");
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





}
