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
import io.github.clescot.kafka.connect.http.sink.client.HttpException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.HTTP_CLIENT_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX;


public class HttpSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    public static final String HEADER_X_CORRELATION_ID = "X-Correlation-ID";
    public static final String HEADER_X_REQUEST_ID = "X-Request-ID";
    public static final String SINK_RECORD_HAS_GOT_A_NULL_VALUE = "sinkRecord has got a 'null' value";
    public static final String DEFAULT_CONFIGURATION_ID = "default";


    private Queue<KafkaRecord> queue;
    private String queueName;

    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private ErrantRecordReporter errantRecordReporter;

    private List<Configuration> customConfigurations;
    private static ExecutorService executor;
    private Configuration defaultConfiguration;
    private final Pattern defaultSuccessPattern = Pattern.compile(HTTP_CLIENT_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);

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

        Integer customFixedThreadPoolSize = httpSinkConnectorConfig.getCustomFixedThreadpoolSize();
        if(customFixedThreadPoolSize!=null &&executor==null){
            executor = Executors.newFixedThreadPool(customFixedThreadPoolSize);
        }

        this.defaultConfiguration = new Configuration(DEFAULT_CONFIGURATION_ID,httpSinkConnectorConfig,executor);
        customConfigurations = buildCustomConfigurations(httpSinkConnectorConfig,defaultConfiguration,executor);


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

    private List<Configuration> buildCustomConfigurations(HttpSinkConnectorConfig httpSinkConnectorConfig,
                                                          Configuration defaultConfiguration,
                                                          ExecutorService executorService){
        CopyOnWriteArrayList<Configuration> configurations = Lists.newCopyOnWriteArrayList();
        for (String configId: httpSinkConnectorConfig.getConfigurationIds()) {
           Configuration configuration = new Configuration(configId,httpSinkConnectorConfig,executorService);
           if(configuration.getHttpClient()==null) {
               configuration.setHttpClient(defaultConfiguration.getHttpClient());
           }
           if(configuration.getRetryPolicy().isEmpty()&&defaultConfiguration.getRetryPolicy().isPresent()){
               configuration.setRetryPolicy(defaultConfiguration.getRetryPolicy().get());
           }
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
        Preconditions.checkNotNull(defaultConfiguration, "defaultConfiguration is null. 'start' method must be called once before put");

        //we submit futures to the pool
        List<CompletableFuture<HttpExchange>> completableFutures = records.stream().map(this::process).collect(Collectors.toList());
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());


    }

    private CompletableFuture<HttpExchange> process(SinkRecord sinkRecord) {
        try {
            if (sinkRecord.value() == null) {
                throw new ConnectException("sinkRecord Value is null :" + sinkRecord);
            }
            //build HttpRequest
            HttpRequest httpRequest = buildHttpRequest(sinkRecord);

            //is there a matching configuration against the request ?
            Configuration foundConfiguration = customConfigurations
                    .stream()
                    .filter(config -> config.matches(httpRequest))
                    .findFirst()
                    .orElse(defaultConfiguration);
            HttpRequest enhancedHttpRequest = foundConfiguration.addStaticHeaders(httpRequest);
            enhancedHttpRequest = foundConfiguration.addTrackingHeaders(httpRequest);
            //handle Request and Response
            return callWithRetryPolicy(sinkRecord, enhancedHttpRequest, foundConfiguration).thenApply(
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

    private CompletableFuture<HttpExchange> callWithRetryPolicy(SinkRecord sinkRecord,
                                                                HttpRequest httpRequest,
                                                                Configuration configuration) {
        Optional<RetryPolicy<HttpExchange>> retryPolicyForCall = configuration.getRetryPolicy();
        if (httpRequest != null) {
            AtomicInteger attempts = new AtomicInteger();
            try {
                attempts.addAndGet(HttpClient.ONE_HTTP_REQUEST);
                if (retryPolicyForCall.isPresent()) {
                    RetryPolicy<HttpExchange> retryPolicy = retryPolicyForCall.get();
                    CompletableFuture<HttpExchange> httpExchangeFuture = callAndPublish(sinkRecord, httpRequest, attempts,configuration)
                       .thenApply(httpExchange-> handleRetry(httpExchange,configuration)
                    );
                    return Failsafe.with(List.of(retryPolicy))
                       .getStageAsync(()->httpExchangeFuture);
                } else {
                    return callAndPublish(sinkRecord, httpRequest, attempts,configuration);
                }
            } catch (Throwable throwable) {
                LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, throwable,
                        throwable.getMessage());
                return CompletableFuture.supplyAsync(() -> defaultConfiguration.getHttpClient().buildHttpExchange(
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

    private HttpExchange handleRetry(HttpExchange httpExchange,Configuration configuration) {
        //we don't retry success HTTP Exchange
        boolean responseCodeImpliesRetry = retryNeeded(httpExchange.getHttpResponse(),configuration);
        LOGGER.debug("httpExchange success :'{}'", httpExchange.isSuccess());
        LOGGER.debug("response code('{}') implies retry:'{}'", httpExchange.getHttpResponse().getStatusCode(), "" + responseCodeImpliesRetry);
        if (!httpExchange.isSuccess()
                && responseCodeImpliesRetry) {
            throw new HttpException(httpExchange, "retry needed");
        }
        return httpExchange;
    }


    protected boolean retryNeeded(HttpResponse httpResponse,Configuration configuration) {
        Optional<Pattern> retryResponseCodeRegex = configuration.getRetryResponseCodeRegex();
        if(retryResponseCodeRegex.isPresent()) {
            Pattern retryPattern = retryResponseCodeRegex.get();
            Matcher matcher = retryPattern.matcher("" + httpResponse.getStatusCode());
            return matcher.matches();
        }else {
            return false;
        }
    }

    private CompletableFuture<HttpExchange> callWithThrottling(HttpRequest httpRequest, AtomicInteger attempts,Configuration configuration) {
        try {
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getRateLimiter();
            if(rateLimiter.isPresent()) {
                rateLimiter.get().acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                LOGGER.debug("permits acquired request:'{}'", httpRequest);
            }
            return configuration.getHttpClient().call(httpRequest, attempts);
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire execution permit from the rate limiter {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        }
    }


    private CompletableFuture<HttpExchange> callAndPublish(SinkRecord sinkRecord,
                                                           HttpRequest httpRequest,
                                                           AtomicInteger attempts,
                                                           Configuration configuration) {
        return callWithThrottling(httpRequest, attempts,configuration)
                .thenApply(myHttpExchange -> {
                    boolean success = isSuccess(myHttpExchange,configuration);
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

    protected boolean isSuccess(HttpExchange httpExchange,Configuration configuration) {
        Pattern pattern = defaultSuccessPattern;
        if(configuration.getSuccessResponseCodeRegex().isPresent()) {
            pattern = configuration.getSuccessResponseCodeRegex().get();
        }
        return pattern.matcher(httpExchange.getHttpResponse().getStatusCode() + "").matches();

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

    @Override
    public void stop() {
        //Producer are stopped in connector stop
    }

    protected void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }

    public Configuration getDefaultConfiguration() {
        return defaultConfiguration;
    }
}
