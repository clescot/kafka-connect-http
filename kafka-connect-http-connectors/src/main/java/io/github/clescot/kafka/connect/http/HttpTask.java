package io.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class HttpTask<T extends ConnectRecord<T>> {

    public static final String DEFAULT_CONFIGURATION_ID = "default";
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);
    public static final String SINK_RECORD_HAS_GOT_A_NULL_VALUE = "sinkRecord has got a 'null' value";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static ExecutorService executorService;
    private final Configuration defaultConfiguration;
    private final boolean publishToInMemoryQueue;
    private final String queueName;
    private Queue<KafkaRecord> queue;
    private static CompositeMeterRegistry meterRegistry;
    private final List<Configuration> customConfigurations;

    public HttpTask(AbstractConfig config) {
        if (meterRegistry == null) {
            HttpTask.meterRegistry = buildMeterRegistry(config);
        }
        Integer customFixedThreadPoolSize = Optional.ofNullable(config.getInt(CONFIG_HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE)).orElse(1);
        if (executorService == null) {
            setThreadPoolSize(customFixedThreadPoolSize);
        }
        bindMetrics(config, meterRegistry, executorService);
        this.defaultConfiguration = new Configuration(DEFAULT_CONFIGURATION_ID, config, executorService, meterRegistry);
        this.publishToInMemoryQueue = Boolean.parseBoolean(config.getString(PUBLISH_TO_IN_MEMORY_QUEUE));
        this.queueName = Optional.ofNullable(config.getString(ConfigConstants.QUEUE_NAME)).orElse(QueueFactory.DEFAULT_QUEUE_NAME);
        this.queue = QueueFactory.getQueue(queueName);
        this.customConfigurations = buildCustomConfigurations(config, defaultConfiguration, executorService);
    }

    private List<Configuration> buildCustomConfigurations(AbstractConfig config,
                                                          Configuration defaultConfiguration,
                                                          ExecutorService executorService) {
        CopyOnWriteArrayList<Configuration> configurations = Lists.newCopyOnWriteArrayList();

        for (String configId : Optional.ofNullable(config.getList(CONFIGURATION_IDS)).orElse(Lists.newArrayList())) {
            Configuration configuration = new Configuration(configId, config, executorService, meterRegistry);
            if (configuration.getHttpClient() == null) {
                configuration.setHttpClient(defaultConfiguration.getHttpClient());
            }

            //we reuse the default retry policy if not set
            Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = defaultConfiguration.getRetryPolicy();
            if (configuration.getRetryPolicy().isEmpty() && defaultRetryPolicy.isPresent()) {
                configuration.setRetryPolicy(defaultRetryPolicy.get());
            }
            //we reuse the default success response code regex if not set
            configuration.setSuccessResponseCodeRegex(defaultConfiguration.getSuccessResponseCodeRegex());

            Optional<Pattern> defaultRetryResponseCodeRegex = defaultConfiguration.getRetryResponseCodeRegex();
            if (configuration.getRetryResponseCodeRegex().isEmpty() && defaultRetryResponseCodeRegex.isPresent()) {
                configuration.setRetryResponseCodeRegex(defaultRetryResponseCodeRegex.get());
            }

            configurations.add(configuration);
        }
        return configurations;
    }

    protected HttpRequest buildHttpRequest(ConnectRecord<T> sinkRecord) throws ConnectException {
        if (sinkRecord == null || sinkRecord.value() == null) {
            LOGGER.warn(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
            throw new ConnectException(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
        }
        HttpRequest httpRequest = null;
        Object value = sinkRecord.value();
        Class<?> valueClass = value.getClass();
        String stringValue = null;

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
            LOGGER.warn("value is an instance of the class '{}' not handled by the WsSinkTask", valueClass.getName());
            throw new ConnectException("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
        }
        if (httpRequest == null) {
            LOGGER.debug("stringValue :{}", stringValue);
            httpRequest = parseHttpRequestAsJsonString(stringValue);
            LOGGER.debug("successful httpRequest parsing :{}", httpRequest);
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


    private CompletableFuture<HttpExchange> callAndPublish(ConnectRecord<T> sinkRecord,
                                                           HttpRequest httpRequest,
                                                           AtomicInteger attempts,
                                                           Configuration configuration) {
        if(LOGGER.isTraceEnabled()){
            LOGGER.trace("before enrichment:{}",httpRequest);
        }
        HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
        if(LOGGER.isTraceEnabled()){
            LOGGER.trace("after enrichment:{}",enrichedHttpRequest);
        }
        CompletableFuture<HttpExchange> completableFuture = configuration.getHttpClient().call(enrichedHttpRequest, attempts);
        return completableFuture
                .thenApply(myHttpExchange -> {
                    HttpExchange enrichedHttpExchange = configuration.enrich(myHttpExchange);

                    //publish eventually to 'in memory' queue
                    if (this.publishToInMemoryQueue) {
                        LOGGER.debug("http exchange published to queue '{}':{}", queueName, enrichedHttpExchange);
                        queue.offer(new KafkaRecord(sinkRecord.headers(), sinkRecord.keySchema(), sinkRecord.key(), enrichedHttpExchange));
                    } else {
                        LOGGER.debug("http exchange NOT published to queue '{}':{}", queueName, enrichedHttpExchange);
                    }
                    return enrichedHttpExchange;
                });

    }

    private CompletableFuture<HttpExchange> callWithRetryPolicy(ConnectRecord<T> sinkRecord,
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

    public CompletableFuture<HttpExchange> processRecord(ConnectRecord<T> sinkRecord) {
        HttpRequest httpRequest;
        //build HttpRequest
        httpRequest = buildHttpRequest(sinkRecord);

        //is there a matching configuration against the request ?
        Configuration foundConfiguration = customConfigurations
                .stream()
                .filter(config -> config.matches(httpRequest))
                .findFirst()
                .orElse(defaultConfiguration);
        if(LOGGER.isTraceEnabled()){
            LOGGER.trace("configuration:{}",foundConfiguration);
        }
        //handle Request and Response
        return callWithRetryPolicy(sinkRecord, httpRequest, foundConfiguration).thenApply(
                myHttpExchange -> {
                    LOGGER.debug("HTTP exchange :{}", myHttpExchange);
                    return myHttpExchange;
                }
        );
    }

    private CompositeMeterRegistry buildMeterRegistry(AbstractConfig config) {
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        boolean activateJMX = Boolean.parseBoolean(config.getString(METER_REGISTRY_EXPORTER_JMX_ACTIVATE));
        if (activateJMX) {
            JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
            jmxMeterRegistry.start();
            compositeMeterRegistry.add(jmxMeterRegistry);
        }
        boolean activatePrometheus = Boolean.parseBoolean(config.getString(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE));
        if (activatePrometheus) {
            PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Integer prometheusPort = config.getInt(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT);
            // you can set the daemon flag to false if you want the server to block
            HTTPServer httpServer = null;
            try {
                httpServer = new HTTPServer(new InetSocketAddress(prometheusPort != null ? prometheusPort : 9090), prometheusRegistry.getPrometheusRegistry(), true);
            } catch (IOException e) {
                throw new HttpException(e);
            } finally {
                if (httpServer != null) {
                    httpServer.close();
                }
            }
            compositeMeterRegistry.add(prometheusRegistry);
        }
        return compositeMeterRegistry;
    }

    private static void bindMetrics(AbstractConfig config, MeterRegistry meterRegistry, ExecutorService myExecutorService) {
        boolean bindExecutorServiceMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE));
        if (bindExecutorServiceMetrics) {
            new ExecutorServiceMetrics(myExecutorService, "HttpSinkTask", Lists.newArrayList()).bindTo(meterRegistry);
        }
        boolean bindJvmMemoryMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_MEMORY));
        if (bindJvmMemoryMetrics) {
            new JvmMemoryMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmThreadMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_THREAD));
        if (bindJvmThreadMetrics) {
            new JvmThreadMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmInfoMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_INFO));
        if (bindJvmInfoMetrics) {
            new JvmInfoMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmGcMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_GC));
        if (bindJvmGcMetrics) {
            try (JvmGcMetrics gcMetrics = new JvmGcMetrics()) {
                gcMetrics.bindTo(meterRegistry);
            }
        }
        boolean bindJVMClassLoaderMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER));
        if (bindJVMClassLoaderMetrics) {
            new ClassLoaderMetrics().bindTo(meterRegistry);
        }
        boolean bindJVMProcessorMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR));
        if (bindJVMProcessorMetrics) {
            new ProcessorMetrics().bindTo(meterRegistry);
        }
        boolean bindLogbackMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_LOGBACK));
        if (bindLogbackMetrics) {
            try (LogbackMetrics logbackMetrics = new LogbackMetrics()) {
                logbackMetrics.bindTo(meterRegistry);
            }
        }
    }

    /**
     * define a static field from a non-static method need a static synchronized method
     *
     * @param customFixedThreadPoolSize max thread pool size for the executorService.
     */
    public static synchronized void setThreadPoolSize(Integer customFixedThreadPoolSize) {
        executorService = Executors.newFixedThreadPool(customFixedThreadPoolSize);
    }

    public void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }

    public Configuration getDefaultConfiguration() {
        return defaultConfiguration;
    }

    public static ExecutorService getExecutorService() {
        return executorService;
    }

    public static CompositeMeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public static void removeCompositeMeterRegistry(){
        meterRegistry = null;
    }
}
