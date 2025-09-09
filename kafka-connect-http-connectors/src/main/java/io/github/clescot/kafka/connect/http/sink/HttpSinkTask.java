package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.VersionUtils;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.MessageSplitter;
import io.github.clescot.kafka.connect.http.MessageSplitterFactory;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.client.HttpConfiguration;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.mapper.HttpRequestMapper;
import io.github.clescot.kafka.connect.http.mapper.HttpRequestMapperFactory;
import io.github.clescot.kafka.connect.http.sink.publish.KafkaProducer;
import io.github.clescot.kafka.connect.http.sink.publish.PublishConfigurer;
import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.HTTP_REQUEST_MAPPER_IDS;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.MESSAGE_SPLITTER_IDS;

/**
 * HttpSinkTask is a Kafka Connect SinkTask that processes SinkRecords,
 * splits messages, maps them to HttpRequests, and sends them to an HttpClient.
 *
 * @param <C> type of the HttpClient
 * @param <R> type of the native HttpRequest
 * @param <S> type of the native HttpResponse
 */
public abstract class HttpSinkTask<C extends HttpClient<R, S>, R, S> extends SinkTask {
    public static final BiFunction<SinkRecord, String, SinkRecord> FROM_STRING_PART_TO_SINK_RECORD_FUNCTION = (sinkRecord, string) -> new SinkRecord(
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.keySchema(),
            sinkRecord.key(),
            sinkRecord.valueSchema(),
            string,
            sinkRecord.kafkaOffset(),
            sinkRecord.timestamp(),
            sinkRecord.timestampType()
    );
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private final HttpClientFactory<C, R, S> httpClientFactory;
    private Queue<KafkaRecord> queue;
    private ErrantRecordReporter errantRecordReporter;

    private HttpTask<SinkRecord,C, R, S> httpTask;
    private final KafkaProducer<String, Object> producer;
    private PublishMode publishMode;
    private HttpConnectorConfig httpConnectorConfig;
    private List<MessageSplitter<SinkRecord>> messageSplitters;
    private HttpRequestMapper defaultHttpRequestMapper;
    private List<HttpRequestMapper> httpRequestMappers;

    @SuppressWarnings("java:S5993")
    public HttpSinkTask(HttpClientFactory<C, R, S> httpClientFactory, KafkaProducer<String, Object> producer) {
        this.httpClientFactory = httpClientFactory;
        this.producer = producer;
    }


    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }


    /**
     * @param settings configure the connector
     */
    @Override
    public void start(Map<String, String> settings) {

        try {
            errantRecordReporter = context.errantRecordReporter();
            if (errantRecordReporter == null) {
                LOGGER.warn("Dead Letter Queue (DLQ) is not enabled. it is recommended to configure a Dead Letter Queue for a better error handling.");
            }
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            errantRecordReporter = null;
        }

        Preconditions.checkNotNull(settings, "settings cannot be null");
        HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
        this.httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);
        LOGGER.debug("httpConnectorConfig: {}", httpConnectorConfig);

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
        JexlEngine jexlEngine = buildJexlEngine();

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
                httpConnectorConfig.originalsStrings(),
                jexlEngine,
                httpConnectorConfig.getList(HTTP_REQUEST_MAPPER_IDS)
        );

        //message splitters
        MessageSplitterFactory<SinkRecord> messageSplitterFactory = new MessageSplitterFactory<>(FROM_STRING_PART_TO_SINK_RECORD_FUNCTION);

        this.messageSplitters = messageSplitterFactory.buildMessageSplitters(httpConnectorConfig.originalsStrings(), jexlEngine, httpConnectorConfig.getList(MESSAGE_SPLITTER_IDS));
        httpTask = new HttpTask<>(httpConnectorConfig, httpClientFactory);

    }


    @Override
    @SuppressWarnings("java:S3864")
    public void put(Collection<SinkRecord> records) {
        List<HttpExchange> httpExchanges = putAndGetExchanges(records);
        if (httpExchanges == null) return;
        LOGGER.debug("HttpExchanges created :'{}'", httpExchanges.size());

    }

    public @Nullable List<HttpExchange> putAndGetExchanges(Collection<SinkRecord> records) {
        Preconditions.checkNotNull(records, "records collection to be processed is null");
        if (records.isEmpty()) {
            LOGGER.debug("no records");
            return null;
        }
        Preconditions.checkNotNull(httpTask, "httpTask is null. 'start' method must be called once before put");
        List<Pair<SinkRecord, HttpRequest>> preparedRequests = prepareRequests(records);
        //List<SinkRecord>-> SinkRecord
        List<CompletableFuture<HttpExchange>> completableFutures = preparedRequests.stream()
                .map(this::callAndPublish)
                .toList();
        return completableFutures.stream().map(CompletableFuture::join).toList();
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

    public CompletableFuture<HttpExchange> callAndPublish(Pair<SinkRecord, HttpRequest> pair) {

        return httpTask.call(pair.getRight())
                .thenApply(publish())
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

    private List<SinkRecord> splitMessage(SinkRecord sinkRecord) {
        Optional<MessageSplitter<SinkRecord>> splitterFound = messageSplitters.stream()
                .filter(messageSplitter -> messageSplitter.matches(sinkRecord)).findFirst();
        //splitter
        List<SinkRecord> results;
        if (splitterFound.isPresent()) {
            results = splitterFound.get().split(sinkRecord);
        } else {
            results = List.of(sinkRecord);
        }
        return results;
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

    @SuppressWarnings("java:S3864")
    public List<Pair<SinkRecord, HttpRequest>> prepareRequests(Collection<SinkRecord> records) {
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

        return httpTask.groupRequests(requests);

    }

    private void debugConnectRecord(ConnectRecord<SinkRecord> sinkRecord) {
        Object value = sinkRecord.value();
        if (value != null) {
            Class<?> valueClass = value.getClass();
            LOGGER.debug("valueClass is '{}'", valueClass.getName());
            LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
        }
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
    private KafkaRecord mapToRecord(HttpExchange httpExchange) {
        return new KafkaRecord(null, null, null, httpExchange);
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


    @Override
    public void stop() {
        if (httpTask == null) {
            LOGGER.error("httpTask hasn't been created with the 'start' method");
        }

    }


    public HttpConfiguration<C, R, S> getDefaultConfiguration() {
        Preconditions.checkNotNull(httpTask, "httpTask has not been initialized in the start method");
        return httpTask.getDefaultConfiguration();
    }

    public Map<String,HttpConfiguration<C, R, S>> getConfigurations() {
        Preconditions.checkNotNull(httpTask, "httpTask has not been initialized in the start method");
        return httpTask.getConfigurations();
    }

    public HttpTask<SinkRecord,C, R, S> getHttpTask() {
        return httpTask;
    }


    protected HttpRequestMapper getDefaultHttpRequestMapper() {
        return this.defaultHttpRequestMapper;
    }

    public void setQueue(Queue<KafkaRecord> queue) {
        this.queue =queue;
    }

    public static void clearMeterRegistry() {
        HttpTask.clearMeterRegistry();
    }

}
