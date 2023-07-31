package io.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.sink.client.HttpClient;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class HttpSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final VersionUtils VERSION_UTILS = new VersionUtils();

    public static final String SINK_RECORD_HAS_GOT_A_NULL_VALUE = "sinkRecord has got a 'null' value";
    public static final String DEFAULT_CONFIGURATION_ID = "default";


    private Queue<KafkaRecord> queue;
    private String queueName;

    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private ErrantRecordReporter errantRecordReporter;

    private List<Configuration> customConfigurations;
    private static ExecutorService executorService;
    private Configuration defaultConfiguration;


    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    /**
     * @param settings configure the connector
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
        setThreadPoolSize(customFixedThreadPoolSize);

        MeterRegistry meterRegistry = buildMeterRegistry();
        bindMetrics(meterRegistry);

        this.defaultConfiguration = new Configuration(DEFAULT_CONFIGURATION_ID, httpSinkConnectorConfig, executorService, meterRegistry);
        customConfigurations = buildCustomConfigurations(httpSinkConnectorConfig, defaultConfiguration, executorService, meterRegistry);


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

    private static void bindMetrics(MeterRegistry meterRegistry) {
        new ExecutorServiceMetrics(executorService,"HttpSinkTask",Lists.newArrayList()).bindTo(meterRegistry);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
        new JvmInfoMetrics().bindTo(meterRegistry);
    }

    /**
     * define a static field from a non static method need a static synchronized method
     * @param customFixedThreadPoolSize
     */
    private static synchronized void setThreadPoolSize(Integer customFixedThreadPoolSize) {
        if (customFixedThreadPoolSize != null && executorService == null) {
            executorService = Executors.newFixedThreadPool(customFixedThreadPoolSize);
        }
    }

    private MeterRegistry buildMeterRegistry(){
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        MeterRegistry jmxMeterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM);
        compositeMeterRegistry.add(jmxMeterRegistry);
        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        compositeMeterRegistry.add(prometheusRegistry);
        return compositeMeterRegistry;
    }

    private List<Configuration> buildCustomConfigurations(HttpSinkConnectorConfig httpSinkConnectorConfig,
                                                          Configuration defaultConfiguration,
                                                          ExecutorService executorService,
                                                          MeterRegistry meterRegistry) {
        CopyOnWriteArrayList<Configuration> configurations = Lists.newCopyOnWriteArrayList();
        for (String configId : httpSinkConnectorConfig.getConfigurationIds()) {
            Configuration configuration = new Configuration(configId, httpSinkConnectorConfig, executorService, meterRegistry);
            if (configuration.getHttpClient() == null) {
                configuration.setHttpClient(defaultConfiguration.getHttpClient());
            }

            //we reuse the default retry policy if not set
            if (configuration.getRetryPolicy().isEmpty() && defaultConfiguration.getRetryPolicy().isPresent()) {
                configuration.setRetryPolicy(defaultConfiguration.getRetryPolicy().get());
            }
            //we reuse the default success response code regex if not set
            configuration.setSuccessResponseCodeRegex(defaultConfiguration.getSuccessResponseCodeRegex());

            if (configuration.getRetryResponseCodeRegex().isEmpty() && defaultConfiguration.getRetryResponseCodeRegex().isPresent()) {
                configuration.setRetryResponseCodeRegex(defaultConfiguration.getRetryResponseCodeRegex().get());
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
        LOGGER.debug("HttpExchanges created :'{}'",httpExchanges.size());

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

            //handle Request and Response
            return callWithRetryPolicy(sinkRecord, httpRequest, foundConfiguration).thenApply(
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
                    Thread.currentThread().interrupt();
                    throw new ConnectException(ex);
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
                    CompletableFuture<HttpExchange> httpExchangeFuture = callAndPublish(sinkRecord, httpRequest, attempts, configuration)
                                                                         .thenApply(configuration::handleRetry);
                    return Failsafe.with(List.of(retryPolicy)).getStageAsync(() -> httpExchangeFuture);
                } else {
                    return callAndPublish(sinkRecord, httpRequest, attempts, configuration);
                }
            } catch (Exception exception) {
                LOGGER.error("Failed to call web service after {} retries with error({}). message:{} ", attempts, exception,
                        exception.getMessage());
                return CompletableFuture.supplyAsync(() -> defaultConfiguration.getHttpClient().buildHttpExchange(
                        httpRequest,
                        new HttpResponse(HttpClient.SERVER_ERROR_STATUS_CODE, String.valueOf(exception.getMessage())),
                        Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                        attempts,
                        HttpClient.FAILURE));
            }
        } else {
            throw new IllegalArgumentException("httpRequest is null");
        }
    }


    private CompletableFuture<HttpExchange> callAndPublish(SinkRecord sinkRecord,
                                                           HttpRequest httpRequest,
                                                           AtomicInteger attempts,
                                                           Configuration configuration) {
        HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
        CompletableFuture<HttpExchange> completableFuture = configuration.getHttpClient().call(enrichedHttpRequest, attempts);
        return completableFuture
                .thenApply(myHttpExchange -> {
                    HttpExchange enrichedHttpExchange = configuration.enrich(myHttpExchange);

                    //publish eventually to 'in memory' queue
                    if (httpSinkConnectorConfig.isPublishToInMemoryQueue()) {
                        LOGGER.debug("http exchange published to queue '{}':{}", queueName, enrichedHttpExchange);
                        queue.offer(new KafkaRecord(sinkRecord.headers(), sinkRecord.keySchema(), sinkRecord.key(), enrichedHttpExchange));
                    } else {
                        LOGGER.debug("http exchange NOT published to queue '{}':{}", queueName, enrichedHttpExchange);
                    }
                    return enrichedHttpExchange;
                });

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
            } else if (byte[].class.isAssignableFrom(valueClass)) {
                //we assume the value is a byte array
                stringValue = new String((byte[]) value, StandardCharsets.UTF_8);
                LOGGER.debug("byte[] is {}", stringValue);
            } else if (String.class.isAssignableFrom(valueClass)) {
                stringValue = (String) value;
                LOGGER.debug("String is {}", stringValue);
            } else {
                LOGGER.warn("value is an instance of the class '{}' not handled by the WsSinkTask",valueClass.getName());
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
            throw new ConnectException(e);
        }
        return httpRequest;
    }

    @Override
    public void stop() {
        if (!executorService.isShutdown()) {
            executorService.shutdown();
        }
        try {
            boolean awaitTermination = executorService.awaitTermination(30, TimeUnit.SECONDS);
            if(!awaitTermination) {
                LOGGER.warn("timeout elapsed before executor termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException(e);
        }
        LOGGER.info("executor is shutdown : '{}'", executorService.isShutdown());
        LOGGER.info("executor tasks are terminated : '{}'", executorService.isTerminated());
    }

    protected void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }

    public Configuration getDefaultConfiguration() {
        return defaultConfiguration;
    }
}
