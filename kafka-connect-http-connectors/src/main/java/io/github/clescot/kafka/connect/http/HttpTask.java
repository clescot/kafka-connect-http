package io.github.clescot.kafka.connect.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.Task;
import io.github.clescot.kafka.connect.http.client.*;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.mapper.HttpRequestMapper;
import io.github.clescot.kafka.connect.http.mapper.HttpRequestMapperFactory;
import io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import io.github.clescot.kafka.connect.http.sink.publish.KafkaProducer;
import io.github.clescot.kafka.connect.http.sink.publish.PublishConfigurer;
import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.CONFIGURATION_IDS;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigurationFactory.buildConfigurations;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

/**
 *
 * @param <C> client type, which is a subclass of HttpClient
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 */
public class HttpTask<C extends HttpClient<R,S>,R, S> implements Task<C,HttpConfiguration<C,R,S>,HttpRequest, HttpResponse> {


    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);
    private PublishMode publishMode;

    private Queue<KafkaRecord> queue;
    private final Map<String,HttpConfiguration<C,R, S>> configurations;
    private static CompositeMeterRegistry meterRegistry;
    private HttpConnectorConfig httpConnectorConfig;
    private KafkaProducer<String, Object> producer;
    private HttpRequestMapper defaultHttpRequestMapper;
    private List<HttpRequestMapper> httpRequestMappers;

    private ExecutorService executorService;
    private List<MessageSplitter> messageSplitters;
    private List<RequestGrouper> requestGroupers;

    public HttpTask(Map<String, String> settings,
                    HttpClientFactory<C, R, S> httpClientFactory,
                    KafkaProducer<String, Object> producer) {
        this.producer = producer;

        Preconditions.checkNotNull(settings, "settings cannot be null");
        HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
        this.httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);

        //build executorService
        Optional<Integer> customFixedThreadPoolSize = Optional.ofNullable(httpConnectorConfig.getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE));
        customFixedThreadPoolSize.ifPresent(integer -> this.executorService = buildExecutorService(integer));

        //build meterRegistry
        MeterRegistryFactory meterRegistryFactory = new MeterRegistryFactory();
        setMeterRegistry(meterRegistryFactory.buildMeterRegistry(httpConnectorConfig.originalsStrings()));

        //build httpRequestMappers

        JexlEngine jexlEngine = buildJexlEngine();

        //message splitters
        MessageSplitterFactory messageSplitterFactory = new MessageSplitterFactory();
        this.messageSplitters = messageSplitterFactory.buildMessageSplitters(httpConnectorConfig.originals(), jexlEngine, httpConnectorConfig.getList(MESSAGE_SPLITTER_IDS));

        //HttpRequestMappers
        HttpRequestMapperFactory httpRequestMapperFactory = new HttpRequestMapperFactory();
        this.defaultHttpRequestMapper = httpRequestMapperFactory.buildDefaultHttpRequestMapper(
                jexlEngine,
                httpConnectorConfig.getDefaultRequestMapperMode(),
                httpConnectorConfig.getDefaultUrlExpression(),
                httpConnectorConfig.getDefaultMethodExpression(),
                httpConnectorConfig.getDefaultBodyTypeExpression(),
                httpConnectorConfig.getDefaultBodyExpression(),
                httpConnectorConfig.getDefaultHeadersExpression());
        this.httpRequestMappers = httpRequestMapperFactory.buildCustomHttpRequestMappers(
                httpConnectorConfig.originals(),
                jexlEngine,
                httpConnectorConfig.getList(HTTP_REQUEST_MAPPER_IDS)
        );

        //request groupers
        RequestGrouperFactory requestGrouperFactory = new RequestGrouperFactory();
        this.requestGroupers = requestGrouperFactory.buildRequestGroupers(httpConnectorConfig, httpConnectorConfig.getList(REQUEST_GROUPER_IDS));

        //configurations
        Map<String,HttpClientConfiguration<C, R, S>> httpClientConfigurations = buildConfigurations(
                httpClientFactory,
                executorService,
                httpConnectorConfig.getList(CONFIGURATION_IDS),
                httpConnectorConfig.originals(), meterRegistry
        );
        //wrap configurations in HttpConfiguration
        this.configurations = httpClientConfigurations.entrySet().stream()
                .map(
                        entry->Map.entry(entry.getKey(),
                        new HttpConfiguration<>(entry.getValue())
                        )
                )
                .collect(
                        Collectors.<Map.Entry<String, HttpConfiguration<C,R,S>>, String, HttpConfiguration<C, R, S>>toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue)
                );
        //configure publishMode
        this.publishMode = httpConnectorConfig.getPublishMode();
        LOGGER.debug("publishMode: {}", publishMode);
        PublishConfigurer publishConfigurer = PublishConfigurer.build();
        switch (publishMode) {
            case PRODUCER:
                publishConfigurer.configureProducerPublishMode(httpConnectorConfig, producer);
                break;
            case IN_MEMORY_QUEUE:
                this.queue = publishConfigurer.configureInMemoryQueue(httpConnectorConfig);
                break;
            case NONE:
            default:
                LOGGER.debug("NONE publish mode");
        }
    }

    public HttpTask(Map<String,String> config,
                    Map<String,HttpConfiguration<C,R, S>> configurations,
                    CompositeMeterRegistry meterRegistry) {

        if (HttpTask.meterRegistry == null) {
            HttpTask.meterRegistry = meterRegistry;
        }
        //bind metrics to MeterRegistry and ExecutorService
        bindMetrics(config, meterRegistry, executorService);
        this.configurations = configurations;
    }

    /**
     * get the Configuration matching the HttpRequest, and do the Http call with a retry policy.
     * @param httpRequest http request
     * @return a future of the HttpExchange (complete request and response informations).
     */
    public CompletableFuture<HttpExchange> call(@NotNull HttpRequest httpRequest) {
        HttpConfiguration<C,R, S> foundConfiguration = selectConfiguration(httpRequest);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("configuration:{}", foundConfiguration);
        }
        //handle Request and Response
        return foundConfiguration.call(httpRequest)
                .thenApply(
                        httpExchange -> {
                            LOGGER.debug("HTTP exchange :{}", httpExchange);
                            return httpExchange;
                        }
                );
    }

    private static void bindMetrics(Map<String,String> config, MeterRegistry meterRegistry, ExecutorService myExecutorService) {
        boolean bindExecutorServiceMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE));
        if (bindExecutorServiceMetrics) {
            new ExecutorServiceMetrics(myExecutorService, "HttpSinkTask", Lists.newArrayList()).bindTo(meterRegistry);
        }
        boolean bindJvmMemoryMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_MEMORY));
        if (bindJvmMemoryMetrics) {
            new JvmMemoryMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmThreadMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_THREAD));
        if (bindJvmThreadMetrics) {
            new JvmThreadMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmInfoMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_INFO));
        if (bindJvmInfoMetrics) {
            new JvmInfoMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmGcMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_GC));
        if (bindJvmGcMetrics) {
            try (JvmGcMetrics gcMetrics = new JvmGcMetrics()) {
                gcMetrics.bindTo(meterRegistry);
            }
        }
        boolean bindJVMClassLoaderMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER));
        if (bindJVMClassLoaderMetrics) {
            new ClassLoaderMetrics().bindTo(meterRegistry);
        }
        boolean bindJVMProcessorMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR));
        if (bindJVMProcessorMetrics) {
            new ProcessorMetrics().bindTo(meterRegistry);
        }
        boolean bindLogbackMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_LOGBACK));
        if (bindLogbackMetrics) {
            try (LogbackMetrics logbackMetrics = new LogbackMetrics()) {
                logbackMetrics.bindTo(meterRegistry);
            }
        }
    }


    @Override
    public Map<String,HttpConfiguration<C,R, S>> getConfigurations() {
        return Optional.ofNullable(configurations).orElse(Maps.newHashMap());
    }

    public static synchronized CompositeMeterRegistry getMeterRegistry() {
        return HttpTask.meterRegistry;
    }

    public static void removeCompositeMeterRegistry() {
        HttpTask.meterRegistry = null;
    }

    public HttpConfiguration<C, R, S> getDefaultConfiguration() {
        if( configurations != null && !configurations.isEmpty()) {
            return configurations.get(DEFAULT_CONFIGURATION_ID);
        }
        return null;
    }

    /**
     * @param customFixedThreadPoolSize max thread pool size for the executorService.
     * @return executorService
     */
    private ExecutorService buildExecutorService(Integer customFixedThreadPoolSize) {
        return Executors.newFixedThreadPool(customFixedThreadPoolSize);
    }

    private List<Pair<ConnectRecord, HttpRequest>> groupRequests(List<Pair<ConnectRecord, HttpRequest>> pairList) {
        if (requestGroupers != null && !requestGroupers.isEmpty()) {
            return requestGroupers.stream().map(requestGrouper -> requestGrouper.group(pairList)).reduce(Lists.newArrayList(), (l, r) -> {
                l.addAll(r);
                return l;
            });
        } else {
            return pairList;
        }
    }

    private List<ConnectRecord> splitMessage(ConnectRecord sinkRecord) {
        Optional<MessageSplitter> splitterFound = messageSplitters.stream()
                .filter(messageSplitter -> messageSplitter.matches(sinkRecord)).findFirst();
        //splitter
        List<ConnectRecord> results;
        if (splitterFound.isPresent()) {
            results = splitterFound.get().split(sinkRecord);
        } else {
            results = List.of(sinkRecord);
        }
        return results;
    }

    private @NotNull Pair<ConnectRecord, HttpRequest> toHttpRequests(ConnectRecord sinkRecord) {
        HttpRequestMapper httpRequestMapper = httpRequestMappers.stream()
                .filter(mapper -> mapper.matches(sinkRecord))
                .findFirst()
                .orElse(defaultHttpRequestMapper);

        //build HttpRequest
        HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);

        return Pair.of(sinkRecord, httpRequest);
    }


    @NotNull
    public Function<HttpExchange, HttpExchange> publish() throws HttpException {
        return httpExchange -> {
            //publish eventually to 'in memory' queue
            if (PublishMode.IN_MEMORY_QUEUE.equals(publishMode)) {
                publishInInMemoryQueueMode(httpExchange, this.httpConnectorConfig.getQueueName());
            } else if (PublishMode.PRODUCER.equals(publishMode)) {
                publishInProducerMode(httpExchange, this.httpConnectorConfig.getProducerContent(),
                        this.httpConnectorConfig.getProducerSuccessTopic(),
                        this.httpConnectorConfig.getProducerErrorTopic());
            } else {
                LOGGER.debug("publish.mode : 'NONE' http exchange NOT published :'{}'", httpExchange);
            }
            return httpExchange;
        };
    }

    private void publishInInMemoryQueueMode(HttpExchange httpExchange, String queueName) {
        LOGGER.debug("publish.mode : 'IN_MEMORY_QUEUE': http exchange published to queue '{}':{}", queueName, httpExchange);
        boolean offer = queue.offer(mapToRecord(httpExchange));
        if (!offer) {
            LOGGER.error("sinkRecord  not added to the 'in memory' queue:{}",
                    queueName
            );
        }
    }

    @NotNull
    private KafkaRecord mapToRecord(HttpExchange httpExchange) {
        return new KafkaRecord(null, null, null, httpExchange);
    }

    private void publishInProducerMode(HttpExchange httpExchange,
                                       String producerContent,
                                       String producerSuccessTopic,
                                       String producerErrorTopic) {
        LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange success will be published at topic : '{}'", producerSuccessTopic);
        LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange error will be published at topic : '{}'", producerErrorTopic);
        String targetTopic = httpExchange.isSuccess() ? producerSuccessTopic : producerErrorTopic;
        ProducerRecord<String, Object> myRecord = mapToRecord(httpExchange, producerContent, targetTopic);
        LOGGER.trace("before send to {}", targetTopic);
        RecordMetadata recordMetadata;
        try {
            recordMetadata = this.producer.send(myRecord).get(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HttpException(e);
        } catch (Exception e) {
            throw new HttpException(e);
        }
        long offset = recordMetadata.offset();
        int partition = recordMetadata.partition();
        long timestamp = recordMetadata.timestamp();
        String topic = recordMetadata.topic();
        LOGGER.debug("✉✉ record sent ✉✉ : topic:{},partition:{},offset:{},timestamp:{}", topic, partition, offset, timestamp);
    }

    @NotNull
    private ProducerRecord<String, Object> mapToRecord(HttpExchange httpExchange, String producerContent, String targetTopic) {
        ProducerRecord<String, Object> myRecord;
        if ("response".equalsIgnoreCase(producerContent)) {
            myRecord = new ProducerRecord<>(targetTopic, httpExchange.getHttpResponse());
        } else {
            myRecord = new ProducerRecord<>(targetTopic, httpExchange);
        }
        return myRecord;
    }

    public static synchronized void setMeterRegistry(CompositeMeterRegistry compositeMeterRegistry) {
        if (meterRegistry == null) {
            meterRegistry = compositeMeterRegistry;
        }
    }

    public static synchronized void clearMeterRegistry() {
        meterRegistry = null;
    }

    private static JexlEngine buildJexlEngine() {
        // Restricted permissions to a safe set but with URI allowed
        JexlPermissions permissions = new JexlPermissions.ClassPermissions(SinkRecord.class, ConnectRecord.class, HttpRequest.class);
        // Create the engine
        JexlFeatures features = new JexlFeatures()
                .loops(false)
                .sideEffectGlobal(false)
                .sideEffect(false);
        return new JexlBuilder().features(features).permissions(permissions).create();
    }

    public HttpConnectorConfig getHttpSinkConnectorConfig() {
        return httpConnectorConfig;
    }

    public void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }

    public void stop() {
        if (executorService != null) {
            if (!executorService.isShutdown()) {
                executorService.shutdown();
            }
            try {
                boolean awaitTermination = executorService.awaitTermination(30, TimeUnit.SECONDS);
                if (!awaitTermination) {
                    LOGGER.warn("timeout elapsed before executor termination");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException(e);
            }
            LOGGER.info("executor is shutdown : '{}'", executorService.isShutdown());
            LOGGER.info("executor tasks are terminated : '{}'", executorService.isTerminated());
        }
        if (meterRegistry != null) {
            meterRegistry.close();
        }
        if (producer != null) {
            producer.close();
        }
    }

    public List<Pair<ConnectRecord, HttpRequest>> prepareRequests(Collection<? extends ConnectRecord> records) {
        //we submit futures to the pool
        Stream<? extends ConnectRecord> stream = records.stream();
        //split SinkRecord messages, and convert them to HttpRequest
        List<Pair<ConnectRecord, HttpRequest>> requests = stream
                .filter(sinkRecord -> sinkRecord.value() != null)
                .peek(this::debugConnectRecord)
                .map(this::splitMessage)
                .flatMap(List::stream)
                .map(this::toHttpRequests)
                .toList();

        List<Pair<ConnectRecord, HttpRequest>> groupedRequests = groupRequests(requests);
        return groupedRequests;

    }

    private void debugConnectRecord(ConnectRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (value != null) {
            Class<?> valueClass = value.getClass();
            LOGGER.debug("valueClass is '{}'", valueClass.getName());
            LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
        }
    }

    public HttpRequestMapper getDefaultHttpRequestMapper() {
        return defaultHttpRequestMapper;
    }
}
