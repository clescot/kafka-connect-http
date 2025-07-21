package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.*;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.sink.mapper.HttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.mapper.HttpRequestMapperFactory;
import io.github.clescot.kafka.connect.http.sink.publish.KafkaProducer;
import io.github.clescot.kafka.connect.http.sink.publish.PublishConfigurer;
import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
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
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

/**
 * HttpSinkTask is a Kafka Connect SinkTask that processes SinkRecords,
 * splits messages, maps them to HttpRequests, and sends them to an HttpClient.
 *
 * @param <C> type of the HttpClient
 * @param <R> type of the native HttpRequest
 * @param <S> type of the native HttpResponse
 */
public abstract class HttpSinkTask<C extends HttpClient<R, S>, R, S> extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);

    private static final VersionUtils VERSION_UTILS = new VersionUtils();


    public static final String DEFAULT = "default";


    private HttpRequestMapper defaultHttpRequestMapper;
    private List<HttpRequestMapper> httpRequestMappers;
    public static final String DEFAULT_CONFIGURATION_ID = DEFAULT;
    private final HttpClientFactory<C, R, S> httpClientFactory;

    private ErrantRecordReporter errantRecordReporter;
    private HttpTask<C, R, S> httpTask;
    private KafkaProducer<String, Object> producer;
    private Queue<KafkaRecord> queue;
    private PublishMode publishMode;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;

    private static CompositeMeterRegistry meterRegistry;
    private ExecutorService executorService;
    private List<MessageSplitter> messageSplitters;
    private List<RequestGrouper> requestGroupers;

    @SuppressWarnings("java:S5993")
    public HttpSinkTask(HttpClientFactory<C, R, S> httpClientFactory, KafkaProducer<String, Object> producer) {
        this.httpClientFactory = httpClientFactory;
        this.producer = producer;
    }


    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    private List<HttpClientConfiguration<C, R, S>> buildConfigurations(
            HttpClientFactory<C, R, S> httpClientFactory,
            ExecutorService executorService,
            List<String> configIdList,
            Map<String, Object> originals
    ) {
        List<HttpClientConfiguration<C, R, S>> httpClientConfigurations = Lists.newArrayList();
        List<String> configurationIds = Lists.newArrayList();
        Optional<List<String>> ids = Optional.ofNullable(configIdList);
        configurationIds.add(DEFAULT_CONFIGURATION_ID);
        ids.ifPresent(configurationIds::addAll);
        HttpClientConfiguration<C, R, S> defaultHttpClientConfiguration = null;
        Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();
        Optional<Pattern> defaultRetryResponseCodeRegex = Optional.empty();
        for (String configId : configurationIds) {

            HttpClientConfiguration<C, R, S> httpClientConfiguration = new HttpClientConfiguration<>(configId, httpClientFactory, originals, executorService, meterRegistry);
            if (httpClientConfiguration.getClient() == null && !httpClientConfigurations.isEmpty() && defaultHttpClientConfiguration != null) {
                httpClientConfiguration.setHttpClient(defaultHttpClientConfiguration.getClient());
            }

            //we reuse the default retry policy if not set

            if (httpClientConfiguration.getRetryPolicy().isEmpty() && defaultRetryPolicy.isPresent()) {
                httpClientConfiguration.setRetryPolicy(defaultRetryPolicy.get());
            }
            //we reuse the default success response code regex if not set
            if (defaultHttpClientConfiguration != null) {
                httpClientConfiguration.setSuccessResponseCodeRegex(defaultHttpClientConfiguration.getSuccessResponseCodeRegex());
            }

            if (httpClientConfiguration.getRetryResponseCodeRegex().isEmpty() && defaultRetryResponseCodeRegex.isPresent()) {
                httpClientConfiguration.setRetryResponseCodeRegex(defaultRetryResponseCodeRegex.get());
            }
            if (DEFAULT_CONFIGURATION_ID.equals(configId)) {
                defaultHttpClientConfiguration = httpClientConfiguration;
                defaultRetryPolicy = defaultHttpClientConfiguration.getRetryPolicy();
                defaultRetryResponseCodeRegex = defaultHttpClientConfiguration.getRetryResponseCodeRegex();
            }
            httpClientConfigurations.add(httpClientConfiguration);
        }
        return httpClientConfigurations;
    }

    public static synchronized void setMeterRegistry(CompositeMeterRegistry compositeMeterRegistry) {
        if (meterRegistry == null) {
            meterRegistry = compositeMeterRegistry;
        }
    }

    protected static synchronized void clearMeterRegistry() {
        meterRegistry = null;
    }

    /**
     * @param settings configure the connector
     */
    @Override
    public void start(Map<String, String> settings) {
        List<HttpClientConfiguration<C, R, S>> httpClientConfigurations;
        Preconditions.checkNotNull(settings, "settings cannot be null");
        HttpSinkConfigDefinition httpSinkConfigDefinition = new HttpSinkConfigDefinition(settings);
        this.httpSinkConnectorConfig = new HttpSinkConnectorConfig(httpSinkConfigDefinition.config(), settings);

        //build executorService
        Optional<Integer> customFixedThreadPoolSize = Optional.ofNullable(httpSinkConnectorConfig.getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE));
        customFixedThreadPoolSize.ifPresent(integer -> this.executorService = buildExecutorService(integer));

        //build meterRegistry
        MeterRegistryFactory meterRegistryFactory = new MeterRegistryFactory();
        setMeterRegistry(meterRegistryFactory.buildMeterRegistry(httpSinkConnectorConfig.originalsStrings()));

        //build httpRequestMappers

        JexlEngine jexlEngine = buildJexlEngine();

        //message splitters
        MessageSplitterFactory messageSplitterFactory = new MessageSplitterFactory();
        this.messageSplitters = messageSplitterFactory.buildMessageSplitters(httpSinkConnectorConfig.originals(), jexlEngine, httpSinkConnectorConfig.getList(MESSAGE_SPLITTER_IDS));

        //HttpRequestMappers
        HttpRequestMapperFactory httpRequestMapperFactory = new HttpRequestMapperFactory();
        this.defaultHttpRequestMapper = httpRequestMapperFactory.buildDefaultHttpRequestMapper(
                jexlEngine,
                httpSinkConnectorConfig.getDefaultRequestMapperMode(),
                httpSinkConnectorConfig.getDefaultUrlExpression(),
                httpSinkConnectorConfig.getDefaultMethodExpression(),
                httpSinkConnectorConfig.getDefaultBodyTypeExpression(),
                httpSinkConnectorConfig.getDefaultBodyExpression(),
                httpSinkConnectorConfig.getDefaultHeadersExpression());
        this.httpRequestMappers = httpRequestMapperFactory.buildCustomHttpRequestMappers(
                httpSinkConnectorConfig.originals(),
                jexlEngine,
                httpSinkConnectorConfig.getList(HTTP_REQUEST_MAPPER_IDS)
        );

        //request groupers
        RequestGrouperFactory requestGrouperFactory = new RequestGrouperFactory();
        this.requestGroupers = requestGrouperFactory.buildRequestGroupers(httpSinkConnectorConfig, httpSinkConnectorConfig.getList(REQUEST_GROUPER_IDS));

        //configurations
        httpClientConfigurations = buildConfigurations(
                httpClientFactory,
                executorService,
                httpSinkConnectorConfig.getList(CONFIGURATION_IDS),
                httpSinkConnectorConfig.originals()
        );
        //wrap configurations in HttpConfiguration
        List<HttpConfiguration<C, R, S>> httpConfigurations = httpClientConfigurations.stream()
                .map(HttpConfiguration::new)
                .toList();
        Map<String, String> config = httpSinkConnectorConfig.originalsStrings();
        httpTask = new HttpTask<>(config, httpConfigurations, meterRegistry, executorService);

        try {
            errantRecordReporter = context.errantRecordReporter();
            if (errantRecordReporter == null) {
                LOGGER.warn("Dead Letter Queue (DLQ) is not enabled. it is recommended to configure a Dead Letter Queue for a better error handling.");
            }
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            errantRecordReporter = null;
        }

        //configure publishMode
        this.publishMode = httpSinkConnectorConfig.getPublishMode();
        LOGGER.debug("publishMode: {}", publishMode);
        PublishConfigurer publishConfigurer = PublishConfigurer.build();
        switch (publishMode) {
            case PRODUCER:
                publishConfigurer.configureProducerPublishMode(httpSinkConnectorConfig, producer);
                break;
            case IN_MEMORY_QUEUE:
                this.queue = publishConfigurer.configureInMemoryQueue(httpSinkConnectorConfig);
                break;
            case NONE:
            default:
                LOGGER.debug("NONE publish mode");
        }
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

    /**
     * @param customFixedThreadPoolSize max thread pool size for the executorService.
     * @return executorService
     */
    public ExecutorService buildExecutorService(Integer customFixedThreadPoolSize) {
        return Executors.newFixedThreadPool(customFixedThreadPoolSize);
    }


    @Override
    @SuppressWarnings("java:S3864")
    public void put(Collection<SinkRecord> records) {
        Preconditions.checkNotNull(records, "records collection to be processed is null");
        if (records.isEmpty()) {
            LOGGER.debug("no records");
            return;
        }
        Preconditions.checkNotNull(httpTask, "httpTask is null. 'start' method must be called once before put");
        //we submit futures to the pool
        Stream<SinkRecord> stream = records.stream();
        //split SinkRecord messages, and convert them to HttpRequest
        List<Pair<SinkRecord, HttpRequest>> requests = stream
                .filter(sinkRecord -> sinkRecord.value() != null)
                .peek(this::debugConnectRecord)
                .map(this::splitMessage)
                .flatMap(List::stream)
                .map(this::toHttpRequests)
                .toList();

        List<Pair<SinkRecord, HttpRequest>> groupedRequests = groupRequests(requests);
        //List<SinkRecord>-> SinkRecord
        List<CompletableFuture<HttpExchange>> completableFutures = groupedRequests.stream()
                .map(this::callAndPublish)
                .toList();
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).toList();
        LOGGER.debug("HttpExchanges created :'{}'", httpExchanges.size());

    }

    private List<Pair<SinkRecord, HttpRequest>> groupRequests(List<Pair<SinkRecord, HttpRequest>> pairList) {
        if (requestGroupers != null && !requestGroupers.isEmpty()) {
            return requestGroupers.stream().map(requestGrouper -> requestGrouper.group(pairList)).reduce(Lists.newArrayList(), (l, r) -> {
                l.addAll(r);
                return l;
            });
        } else {
            return pairList;
        }
    }

    private List<SinkRecord> splitMessage(SinkRecord sinkRecord) {
        Optional<MessageSplitter> splitterFound = messageSplitters.stream().filter(messageSplitter -> messageSplitter.matches(sinkRecord)).findFirst();
        //splitter
        List<SinkRecord> results;
        if (splitterFound.isPresent()) {
            results = splitterFound.get().split(sinkRecord);
        } else {
            results = List.of(sinkRecord);
        }
        return results;
    }

    private void debugConnectRecord(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if (value != null) {
            Class<?> valueClass = value.getClass();
            LOGGER.debug("valueClass is '{}'", valueClass.getName());
            LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
        }
    }

    private CompletableFuture<HttpExchange> callAndPublish(Pair<SinkRecord, HttpRequest> pair) {

        return httpTask
                .call(pair.getRight())
                .thenApply(
                        publish(this.publishMode, httpSinkConnectorConfig)
                )
                .exceptionally(throwable -> {
                    LOGGER.error(throwable.getMessage());
                    if (errantRecordReporter != null) {
                        // Send errant record to error reporter
                        Future<Void> future = errantRecordReporter.report(pair.getLeft(), throwable);
                        // Optionally wait until the failure's been recorded in Kafka
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException ex) {
                            Thread.currentThread().interrupt();
                            LOGGER.error(ex.getMessage());
                        }
                    }
                    return null;
                });


    }

    private @NotNull Pair<SinkRecord, HttpRequest> toHttpRequests(SinkRecord sinkRecord) {
        HttpRequestMapper httpRequestMapper = httpRequestMappers.stream()
                .filter(mapper -> mapper.matches(sinkRecord))
                .findFirst()
                .orElse(defaultHttpRequestMapper);

        //build HttpRequest
        HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);

        return Pair.of(sinkRecord, httpRequest);
    }


    private @NotNull Function<HttpExchange, HttpExchange> publish(PublishMode publishMode, HttpSinkConnectorConfig connectorConfig) throws HttpException {
        return httpExchange -> {
            //publish eventually to 'in memory' queue
            if (PublishMode.IN_MEMORY_QUEUE.equals(publishMode)) {
                publishInInMemoryQueueMode(connectorConfig, httpExchange);
            } else if (PublishMode.PRODUCER.equals(publishMode)) {
                publishInProducerMode(connectorConfig, httpExchange);
            } else {
                LOGGER.debug("publish.mode : 'NONE' http exchange NOT published :'{}'", httpExchange);
            }
            return httpExchange;
        };
    }

    private void publishInInMemoryQueueMode(HttpSinkConnectorConfig connectorConfig, HttpExchange httpExchange) {
        LOGGER.debug("publish.mode : 'IN_MEMORY_QUEUE': http exchange published to queue '{}':{}", connectorConfig.getQueueName(), httpExchange);
        boolean offer = queue.offer(mapToRecord(httpExchange));
        if (!offer) {
            LOGGER.error("sinkRecord  not added to the 'in memory' queue:{}",
                    connectorConfig.getQueueName()
            );
        }
    }

    @NotNull
    private KafkaRecord mapToRecord(HttpExchange httpExchange) {
        return new KafkaRecord(null, null, null, httpExchange);
    }

    private void publishInProducerMode(HttpSinkConnectorConfig connectorConfig, HttpExchange httpExchange) {
        LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange success will be published at topic : '{}'", connectorConfig.getProducerSuccessTopic());
        LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange error will be published at topic : '{}'", connectorConfig.getProducerErrorTopic());
        String targetTopic = httpExchange.isSuccess() ? connectorConfig.getProducerSuccessTopic() : connectorConfig.getProducerErrorTopic();
        String producerContent = connectorConfig.getProducerContent();
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

    @Override
    public void stop() {
        if (httpTask == null) {
            LOGGER.error("httpTask hasn't been created with the 'start' method");
            return;
        }
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
    }

    protected void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }

    public HttpConfiguration<C, R, S> getDefaultConfiguration() {
        Preconditions.checkNotNull(httpTask, "httpTask has not been initialized in the start method");
        return httpTask.getDefaultConfiguration();
    }

    public List<HttpConfiguration<C, R, S>> getConfigurations() {
        Preconditions.checkNotNull(httpTask, "httpTask has not been initialized in the start method");
        return httpTask.getConfigurations();
    }

    public HttpTask<C, R, S> getHttpTask() {
        return httpTask;
    }


    protected HttpRequestMapper getDefaultHttpRequestMapper() {
        return defaultHttpRequestMapper;
    }


}
