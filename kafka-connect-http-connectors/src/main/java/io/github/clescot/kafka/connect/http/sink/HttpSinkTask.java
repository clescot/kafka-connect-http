package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.sink.mapper.DirectHttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.mapper.HttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.mapper.JEXLHttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.mapper.MapperMode;
import io.github.clescot.kafka.connect.http.sink.publish.PublishConfigurer;
import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;


public abstract class HttpSinkTask<R, S> extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);

    private static final VersionUtils VERSION_UTILS = new VersionUtils();



    public static final String JEXL_ALWAYS_MATCHES = "true";
    public static final String DEFAULT = "default";
    public static final String MESSAGE_SPLITTER = "message.splitter.";
    public static final String REQUEST_GROUPER = "request.grouper.";
    private Configuration<R, S> defaultConfiguration;
    private List<Configuration<R, S>> customConfigurations;
    private HttpRequestMapper defaultHttpRequestMapper;
    private List<HttpRequestMapper> httpRequestMappers;
    public static final String DEFAULT_CONFIGURATION_ID = DEFAULT;
    private final HttpClientFactory<R, S> httpClientFactory;

    private ErrantRecordReporter errantRecordReporter;
    private HttpTask<SinkRecord, R, S> httpTask;
    private KafkaProducer<String, HttpExchange> producer;
    private Queue<KafkaRecord> queue;
    private PublishMode publishMode;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private static CompositeMeterRegistry meterRegistry;
    private ExecutorService executorService;
    private List<MessageSplitter> messageSplitters;
    private List<RequestGrouper> requestGroupers;
    public HttpSinkTask(HttpClientFactory<R, S> httpClientFactory) {
        this.httpClientFactory = httpClientFactory;
    }

    /**
     * for tests only.
     *
     * @param mock true mock the underlying producer, false not.
     */
    protected HttpSinkTask(HttpClientFactory<R, S> httpClientFactory, boolean mock) {
        this.httpClientFactory = httpClientFactory;
        producer = new KafkaProducer<>(mock);
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    private List<Configuration<R, S>> buildCustomConfigurations(HttpClientFactory<R, S> httpClientFactory,
                                                                AbstractConfig config,
                                                                Configuration<R, S> defaultConfiguration,
                                                                ExecutorService executorService) {
        CopyOnWriteArrayList<Configuration<R, S>> configurations = Lists.newCopyOnWriteArrayList();

        for (String configId : Optional.ofNullable(config.getList(CONFIGURATION_IDS)).orElse(Lists.newArrayList())) {
            Configuration<R, S> configuration = new Configuration<>(configId, httpClientFactory, config, executorService, meterRegistry);
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

    private List<HttpRequestMapper> buildCustomHttpRequestMappers(AbstractConfig config, JexlEngine jexlEngine) {
        List<HttpRequestMapper> requestMappers = Lists.newArrayList();
        for (String httpRequestMapperId : Optional.ofNullable(config.getList(HTTP_REQUEST_MAPPER_IDS)).orElse(Lists.newArrayList())) {
            HttpRequestMapper httpRequestMapper;
            String prefix = "http.request.mapper." + httpRequestMapperId;
            Map<String, Object> settings = config.originalsWithPrefix(prefix);
            String modeKey = ".mode";
            MapperMode mapperMode = MapperMode.valueOf(Optional.ofNullable(settings.get(modeKey)).orElse(MapperMode.DIRECT.name()).toString());
            switch (mapperMode) {
                case JEXL: {
                    httpRequestMapper = new JEXLHttpRequestMapper(
                            httpRequestMapperId,
                            jexlEngine,
                            (String) settings.get(".matcher"),
                            (String) settings.get(".url"),
                            (String) settings.get(".method"),
                            (String) settings.get(".bodytype"),
                            (String) settings.get(".body"),
                            (String) settings.get(".headers")
                    );
                    break;
                }
                case DIRECT:
                default: {
                    httpRequestMapper = new DirectHttpRequestMapper(
                            httpRequestMapperId,
                            jexlEngine,
                            (String) settings.get(".matcher")
                    );
                    break;
                }
            }

            requestMappers.add(httpRequestMapper);
        }
        return requestMappers;
    }


    /**
     * @param settings configure the connector
     */
    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings cannot be null");
        HttpSinkConfigDefinition httpSinkConfigDefinition = new HttpSinkConfigDefinition(settings);
        this.httpSinkConnectorConfig = new HttpSinkConnectorConfig(httpSinkConfigDefinition.config(), settings);


        //build executorService
        Optional<Integer> customFixedThreadPoolSize = Optional.ofNullable(httpSinkConnectorConfig.getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE));
        customFixedThreadPoolSize.ifPresent(integer -> this.executorService = buildExecutorService(integer));

        //build meterRegistry
        HttpSinkTask.meterRegistry = buildMeterRegistry(httpSinkConnectorConfig);

        //build httpRequestMappers

        JexlEngine jexlEngine = buildJexlEngine();

        this.messageSplitters = buildMessageSplitters(httpSinkConnectorConfig,jexlEngine);
        this.defaultHttpRequestMapper = buildDefaultHttpRequestMapper(httpSinkConnectorConfig,jexlEngine);
        this.httpRequestMappers = buildCustomHttpRequestMappers(httpSinkConnectorConfig, jexlEngine);
        this.requestGroupers = buildRequestGroupers(httpSinkConnectorConfig);
        //configurations
        this.defaultConfiguration = new Configuration<>(DEFAULT_CONFIGURATION_ID, httpClientFactory, httpSinkConnectorConfig, executorService, meterRegistry);
        customConfigurations = buildCustomConfigurations(httpClientFactory, httpSinkConnectorConfig, defaultConfiguration, executorService);

        httpTask = new HttpTask<>(httpSinkConnectorConfig, defaultConfiguration, customConfigurations, meterRegistry, executorService);

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
                producer = publishConfigurer.configureProducerPublishMode(httpSinkConnectorConfig);
                break;
            case IN_MEMORY_QUEUE:
                this.queue = publishConfigurer.configureInMemoryQueue(httpSinkConnectorConfig);
                break;
            case NONE:
            default:
                LOGGER.debug("NONE publish mode");
        }

    }

    private List<RequestGrouper> buildRequestGroupers(HttpSinkConnectorConfig connectorConfig) {
        List<RequestGrouper> requestGrouperList = Lists.newArrayList();
        for (String requestGrouperId : Optional.ofNullable(connectorConfig.getList(REQUEST_GROUPER_IDS)).orElse(Lists.newArrayList())) {
            Map<String, Object> settings = connectorConfig.originalsWithPrefix(REQUEST_GROUPER + requestGrouperId + ".");
            Predicate<HttpRequest> httpRequestPredicate = HttpRequestPredicateBuilder.build().buildPredicate(settings);
            String separator = (String) settings.get("separator");
            String start = (String) settings.get("start");
            String end = (String) settings.get("end");
            int messageLimit = (int) settings.get("message.limit");
            RequestGrouper requestGrouper = new RequestGrouper(requestGrouperId,httpRequestPredicate,separator,start,end,messageLimit);
            requestGrouperList.add(requestGrouper);
        }
        return requestGrouperList;
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

    private List<MessageSplitter> buildMessageSplitters(HttpSinkConnectorConfig connectorConfig, JexlEngine jexlEngine) {
        List<MessageSplitter> requestSplitterList = Lists.newArrayList();
        for (String splitterId : Optional.ofNullable(connectorConfig.getList(MESSAGE_SPLITTER_IDS)).orElse(Lists.newArrayList())) {
            Map<String, Object> settings = connectorConfig.originalsWithPrefix(MESSAGE_SPLITTER + splitterId + ".");
            String splitPattern = (String) settings.get("pattern");
            Preconditions.checkNotNull(splitPattern,"message splitter '"+splitterId+"' splitPattern is required");
            String limit = (String) settings.get("limit");
            int splitLimit = 0;
            if(limit!=null&& !limit.isBlank()) {
                splitLimit = Integer.parseInt(limit);
            }
            Map<String, Object> map = connectorConfig.originalsWithPrefix(MESSAGE_SPLITTER + splitterId);
            String matchingExpression = (String) map.get(".matcher");
            MessageSplitter requestSplitter = new MessageSplitter(splitterId,jexlEngine,matchingExpression,splitPattern,splitLimit);
            requestSplitterList.add(requestSplitter);
        }
        return requestSplitterList;
    }

    private HttpRequestMapper buildDefaultHttpRequestMapper(HttpSinkConnectorConfig connectorConfig, JexlEngine jexlEngine){
        HttpRequestMapper httpRequestMapper;
        MapperMode defaultRequestMapperMode = connectorConfig.getDefaultRequestMapperMode();
        switch (defaultRequestMapperMode) {
            case JEXL: {
                Preconditions.checkNotNull(connectorConfig.getDefaultUrlExpression(), "'" + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION + "' need to be set");
                httpRequestMapper = new JEXLHttpRequestMapper(
                        DEFAULT,
                        jexlEngine,
                        JEXL_ALWAYS_MATCHES,
                        connectorConfig.getDefaultUrlExpression(),
                        connectorConfig.getDefaultMethodExpression(),
                        connectorConfig.getDefaultBodyTypeExpression(),
                        connectorConfig.getDefaultBodyExpression(),
                        connectorConfig.getDefaultHeadersExpression()
                );
                break;
            }
            case DIRECT:
            default: {
                httpRequestMapper = new DirectHttpRequestMapper(
                        DEFAULT,
                        jexlEngine,
                        JEXL_ALWAYS_MATCHES
                );
                break;
            }
        }

        return httpRequestMapper;
    }





    /**
     *
     * @param customFixedThreadPoolSize max thread pool size for the executorService.
     * @return executorService
     */
    public ExecutorService buildExecutorService(Integer customFixedThreadPoolSize) {
        return Executors.newFixedThreadPool(customFixedThreadPoolSize);
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
            PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Integer prometheusPort = config.getInt(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT);
            // you can set the daemon flag to false if you want the server to block

            try {
                int port = prometheusPort != null ? prometheusPort : 9090;
                PrometheusRegistry prometheusRegistry = prometheusMeterRegistry.getPrometheusRegistry();
                HTTPServer.builder()
                        .port(port)
                        .registry(prometheusRegistry)
                        .buildAndStart();
            } catch (IOException e) {
                throw new HttpException(e);
            }
            compositeMeterRegistry.add(prometheusMeterRegistry);
        }
        return compositeMeterRegistry;
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

        //TODO regroup messages into one https://github.com/clescot/kafka-connect-http/issues/336
        //predicate on HtpRequest for reducer ?
        //max messages ?
        //max body length ?
        //preserve order ?
        //separator between parts
        //initial separator
        //final separator
        //List<SinkRecord>-> SinkRecord

        List<Pair<SinkRecord, HttpRequest>> requests = stream
                .filter(sinkRecord -> sinkRecord.value() != null)
                .peek(this::debugConnectRecord)
                .map(this::splitMessage)
                .flatMap(List::stream)
                .map(this::toHttpRequests)
                .collect(Collectors.toList());

        List<Pair<SinkRecord, HttpRequest>> groupedRequests = groupRequests(requests);
        //List<SinkRecord>-> SinkRecord
        List<CompletableFuture<HttpExchange>> completableFutures = groupedRequests.stream()
                .map(this::call)
                .collect(Collectors.toList());
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());
        LOGGER.debug("HttpExchanges created :'{}'", httpExchanges.size());

    }

    private List<Pair<SinkRecord, HttpRequest>> groupRequests(List<Pair<SinkRecord, HttpRequest>> pairList){
        if(requestGroupers!=null && !requestGroupers.isEmpty()) {
            return requestGroupers.stream().map(requestGrouper -> requestGrouper.group(pairList)).reduce(Lists.newArrayList(), (l, r) -> {
                l.addAll(r);
                return l;
            });
        }else {
            return pairList;
        }
    }

    private List<SinkRecord> splitMessage(SinkRecord sinkRecord) {
        Optional<MessageSplitter> splitterFound = messageSplitters.stream().filter(messageSplitter -> messageSplitter.matches(sinkRecord)).findFirst();
        //splitter
        List<SinkRecord> results;
        if(splitterFound.isPresent()) {
            results = splitterFound.get().split(sinkRecord);
        }else{
            results = List.of(sinkRecord);
        }
        return results;
    }

    private void debugConnectRecord(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        if(value!=null){
        Class<?> valueClass = value.getClass();
        LOGGER.debug("valueClass is '{}'", valueClass.getName());
        LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
        }
    }

    private CompletableFuture<HttpExchange> call(Pair<SinkRecord, HttpRequest> pair) {

        return httpTask
                .call(pair.getRight())
                .thenApply(
                        publish(pair.getLeft(), this.publishMode, httpSinkConnectorConfig)
                )
                .exceptionally(throwable -> {
                    LOGGER.error(throwable.getMessage());
                    if (errantRecordReporter != null) {
                        // Send errant record to error reporter
                        Future<Void> future = errantRecordReporter.report(pair.getLeft(), throwable);
                        // Optionally wait till the failure's been recorded in Kafka
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




    private @NotNull Function<HttpExchange, HttpExchange> publish(SinkRecord sinkRecord, PublishMode publishMode, HttpSinkConnectorConfig connectorConfig) throws HttpException {
        return httpExchange -> {
            //publish eventually to 'in memory' queue
            if (PublishMode.IN_MEMORY_QUEUE.equals(publishMode)) {
                LOGGER.debug("publish.mode : 'IN_MEMORY_QUEUE': http exchange published to queue '{}':{}", connectorConfig.getQueueName(), httpExchange);
                boolean offer = queue.offer(new KafkaRecord(sinkRecord.headers(), sinkRecord.keySchema(), sinkRecord.key(), httpExchange));
                if (!offer) {
                    LOGGER.error("sinkRecord(topic:{},partition:{},key:{},timestamp:{}) not added to the 'in memory' queue:{}",
                            sinkRecord.topic(),
                            sinkRecord.kafkaPartition(),
                            sinkRecord.key(),
                            sinkRecord.timestamp(),
                            connectorConfig.getQueueName()
                    );
                }
            } else if (PublishMode.PRODUCER.equals(publishMode)) {

                LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange success will be published at topic : '{}'", connectorConfig.getProducerSuccessTopic());
                LOGGER.debug("publish.mode : 'PRODUCER' : HttpExchange error will be published at topic : '{}'", connectorConfig.getProducerErrorTopic());
                String targetTopic = httpExchange.isSuccess() ? connectorConfig.getProducerSuccessTopic() : connectorConfig.getProducerErrorTopic();
                ProducerRecord<String, HttpExchange> myRecord = new ProducerRecord<>(targetTopic, httpExchange);
                LOGGER.trace("before send to {}", targetTopic);
                RecordMetadata recordMetadata;
                try {
                    recordMetadata = this.producer.send(myRecord).get(3, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new HttpException(e);
                }
                long offset = recordMetadata.offset();
                int partition = recordMetadata.partition();
                long timestamp = recordMetadata.timestamp();
                String topic = recordMetadata.topic();
                LOGGER.debug("✉✉ record sent ✉✉ : topic:{},partition:{},offset:{},timestamp:{}", topic, partition, offset, timestamp);

            } else {
                LOGGER.debug("publish.mode : 'NONE' http exchange NOT published :'{}'", httpExchange);
            }
            return httpExchange;
        };
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

    public Configuration<R, S> getDefaultConfiguration() {
        Preconditions.checkNotNull(httpTask, "httpTask has not been initialized in the start method");
        return httpTask.getDefaultConfiguration();
    }

    public HttpTask<SinkRecord, R, S> getHttpTask() {
        return httpTask;
    }


    protected HttpRequestMapper getDefaultHttpRequestMapper() {
        return defaultHttpRequestMapper;
    }
}
