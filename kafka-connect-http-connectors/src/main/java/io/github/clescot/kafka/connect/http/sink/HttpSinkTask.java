package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.*;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.sink.mapper.HttpRequestMapper;
import io.github.clescot.kafka.connect.http.sink.publish.KafkaProducer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

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
    private final HttpClientFactory<C, R, S> httpClientFactory;
    private ErrantRecordReporter errantRecordReporter;

    private HttpTask<C, R, S> httpTask;
    private KafkaProducer<String, Object> producer;



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
        httpTask = new HttpTask<>(settings, httpClientFactory, producer);


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
        List<Pair<ConnectRecord, HttpRequest>> preparedRequests = httpTask.prepareRequests(records);
        //List<SinkRecord>-> SinkRecord
        List<CompletableFuture<HttpExchange>> completableFutures = preparedRequests.stream()
                .map(this::callAndPublish)
                .toList();
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).toList();
        LOGGER.debug("HttpExchanges created :'{}'", httpExchanges.size());

    }

    public CompletableFuture<HttpExchange> callAndPublish(Pair<ConnectRecord, HttpRequest> pair) {

        return httpTask.call(pair.getRight())
                .thenApply(
                        httpTask.publish()
                )
                .exceptionally(throwable -> {
                    LOGGER.error(throwable.getMessage());
                    if (errantRecordReporter != null) {
                        // Send errant record to error reporter
                        Future<Void> future = errantRecordReporter.report((SinkRecord) pair.getLeft(), throwable);
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

    public List<HttpConfiguration<C, R, S>> getConfigurations() {
        Preconditions.checkNotNull(httpTask, "httpTask has not been initialized in the start method");
        return httpTask.getConfigurations();
    }

    public HttpTask<C, R, S> getHttpTask() {
        return httpTask;
    }


    protected HttpRequestMapper getDefaultHttpRequestMapper() {
        return this.httpTask.getDefaultHttpRequestMapper();
    }

    public void setQueue(Queue<KafkaRecord> queue) {
        this.httpTask.setQueue(queue);
    }

    public static void clearMeterRegistry() {
        HttpTask.clearMeterRegistry();
    }

}
