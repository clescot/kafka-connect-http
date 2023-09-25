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
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.CONFIGURATION_IDS;

public class HttpTask<T extends ConnectRecord<T>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);
    public static final String SINK_RECORD_HAS_GOT_A_NULL_VALUE = "sinkRecord has got a 'null' value";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private final Configuration defaultConfiguration;
    private final boolean publishToInMemoryQueue;
    private final String queueName;
    private Queue<KafkaRecord> queue;


    public HttpTask(Configuration defaultConfiguration, boolean publishToInMemoryQueue, String queueName) {
        this.defaultConfiguration = defaultConfiguration;
        this.publishToInMemoryQueue = publishToInMemoryQueue;
        this.queueName = queueName;
        this.queue = QueueFactory.getQueue(queueName);
    }

    public List<Configuration> buildCustomConfigurations(AbstractConfig config,
                                                         Configuration defaultConfiguration,
                                                         ExecutorService executorService,
                                                         MeterRegistry meterRegistry) {
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

    public HttpRequest buildHttpRequest(ConnectRecord<T> sinkRecord) throws ConnectException {
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


    private CompletableFuture<HttpExchange> callAndPublish(ConnectRecord sinkRecord,
                                                           HttpRequest httpRequest,
                                                           AtomicInteger attempts,
                                                           Configuration configuration) {
        HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
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

    public CompletableFuture<HttpExchange> callWithRetryPolicy(ConnectRecord sinkRecord,
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

    public void setQueue(Queue<KafkaRecord> queue) {
        this.queue = queue;
    }
}
