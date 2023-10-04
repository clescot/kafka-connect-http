package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class HttpSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);

    private static final VersionUtils VERSION_UTILS = new VersionUtils();

    private ErrantRecordReporter errantRecordReporter;
    private HttpTask<SinkRecord> httpTask;


    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }

    /**
     * @param settings configure the connector
     */
    @Override
    public void start(Map<String, String> settings) {
        HttpSinkConnectorConfig httpSinkConnectorConfig;
        String queueName;
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

        httpSinkConnectorConfig = new HttpSinkConnectorConfig(HttpSinkConfigDefinition.config(), settings);

        queueName = httpSinkConnectorConfig.getQueueName();
        httpTask = new HttpTask<>(httpSinkConnectorConfig);

        if (httpSinkConnectorConfig.isPublishToInMemoryQueue()) {
            Preconditions.checkArgument(QueueFactory.hasAConsumer(
                    queueName,
                    httpSinkConnectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs()
                    , httpSinkConnectorConfig.getPollDelayRegistrationOfQueueConsumerInMs(),
                    httpSinkConnectorConfig.getPollIntervalRegistrationOfQueueConsumerInMs()
            ), "timeout : '" + httpSinkConnectorConfig.getMaxWaitTimeRegistrationOfQueueConsumerInMs() +
                    "'ms timeout reached :" + queueName + "' queue hasn't got any consumer, " +
                    "i.e no Source Connector has been configured to consume records published in this in memory queue. " +
                    "we stop the Sink Connector to prevent any OutOfMemoryError.");
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {

        Preconditions.checkNotNull(records, "records collection to be processed is null");
        if (records.isEmpty()) {
            return;
        }
        Preconditions.checkNotNull(httpTask, "httpTask is null. 'start' method must be called once before put");

        //we submit futures to the pool
        List<CompletableFuture<HttpExchange>> completableFutures = records.stream().map(this::process).collect(Collectors.toList());
        List<HttpExchange> httpExchanges = completableFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());
        LOGGER.debug("HttpExchanges created :'{}'", httpExchanges.size());

    }

    private CompletableFuture<HttpExchange> process(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        Class<?> valueClass = value.getClass();
        LOGGER.debug("valueClass is '{}'", valueClass.getName());
        LOGGER.debug("value Schema from SinkRecord is '{}'", sinkRecord.valueSchema());
        try {
            if (sinkRecord.value() == null) {
                throw new ConnectException("sinkRecord Value is null :" + sinkRecord);
            }
            return httpTask.processRecord(sinkRecord);
        } catch (ConnectException connectException) {
            LOGGER.error("sink value class is '{}'", valueClass.getName());
            if (errantRecordReporter != null) {
                errantRecordReporter.report(sinkRecord, connectException);
            } else {
                LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            }
            throw connectException;
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



    @Override
    public void stop() {
        if(httpTask==null){
            LOGGER.error("httpTask hasn't been created with the 'start' method");
            return;
        }
        ExecutorService executorService = httpTask.getExecutorService();
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
        this.httpTask.setQueue(queue);
    }

    public Configuration getDefaultConfiguration() {
        return httpTask.getDefaultConfiguration();
    }

    public HttpTask<SinkRecord> getHttpTask() {
        return httpTask;
    }
}
