package io.github.clescot.kafka.connect.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.Task;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.client.HttpConfiguration;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.mapper.HttpRequestMapper;
import io.github.clescot.kafka.connect.http.mapper.HttpRequestMapperFactory;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import io.github.clescot.kafka.connect.http.sink.publish.KafkaProducer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigurationFactory.buildConfigurations;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

/**
 * @param <T> type of the incoming Record, which can be SinkRecord or SourceRecord.
 * @param <C> client type, which is a subclass of HttpClient
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 */
@SuppressWarnings("java:S3740")//we don't want to use the generic of ConnectRecord, to handle both SinkRecord and SourceRecord
public class HttpTask<T extends ConnectRecord<T>,C extends HttpClient<R,S>,R, S> implements Task<C,HttpConfiguration<C,R,S>,HttpRequest, HttpResponse> {


    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);
    


    private final Map<String,HttpConfiguration<C,R, S>> configurations;
    private static CompositeMeterRegistry meterRegistry;
    private KafkaProducer<String, Object> producer;
    private HttpRequestMapper defaultHttpRequestMapper;
    private List<HttpRequestMapper> httpRequestMappers;

    private ExecutorService executorService;
    private List<MessageSplitter<T>> messageSplitters;
    private List<RequestGrouper<T>> requestGroupers;

    public HttpTask(HttpConnectorConfig httpConnectorConfig,
                    HttpClientFactory<C, R, S> httpClientFactory,
                    KafkaProducer<String, Object> producer,
                    BiFunction<T,String,T> fromStringPartToRecordFunction) {
        this.producer = producer;

        //build executorService
        Optional<Integer> customFixedThreadPoolSize = Optional.ofNullable(httpConnectorConfig.getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE));
        customFixedThreadPoolSize.ifPresent(integer -> this.executorService = buildExecutorService(integer));

        //build meterRegistry
        Map<String, String> originalsStrings = httpConnectorConfig.originalsStrings();
        meterRegistry = buildMeterRegistry(originalsStrings);
        bindMetrics(originalsStrings,meterRegistry, executorService);

        JexlEngine jexlEngine = buildJexlEngine();

        //message splitters
        MessageSplitterFactory<T> messageSplitterFactory = new MessageSplitterFactory<>(fromStringPartToRecordFunction);
        this.messageSplitters = messageSplitterFactory.buildMessageSplitters(originalsStrings, jexlEngine, httpConnectorConfig.getList(MESSAGE_SPLITTER_IDS));

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
                originalsStrings,
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
                httpConnectorConfig.getConfigurationIds(),
                originalsStrings, meterRegistry
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


    /**
     * @param customFixedThreadPoolSize max thread pool size for the executorService.
     * @return executorService
     */
    private ExecutorService buildExecutorService(Integer customFixedThreadPoolSize) {
        return Executors.newFixedThreadPool(customFixedThreadPoolSize);
    }

    private List<Pair<T, HttpRequest>> groupRequests(List<Pair<T, HttpRequest>> pairList) {
        if (requestGroupers != null && !requestGroupers.isEmpty()) {
            return requestGroupers.stream()
                    .map(requestGrouper -> requestGrouper.group(pairList))
                    .reduce(Lists.newArrayList(), (l, r) -> {
                l.addAll(r);
                return l;
            });
        } else {
            return pairList;
        }
    }

    private List<T> splitMessage(T sinkRecord) {
        Optional<MessageSplitter<T>> splitterFound = messageSplitters.stream()
                .filter(messageSplitter -> messageSplitter.matches(sinkRecord)).findFirst();
        //splitter
        List<T> results;
        if (splitterFound.isPresent()) {
            results = splitterFound.get().split(sinkRecord);
        } else {
            results = List.of(sinkRecord);
        }
        return results;
    }

    private @NotNull Pair<T, HttpRequest> toHttpRequests(T sinkRecord) {
        HttpRequestMapper httpRequestMapper = httpRequestMappers.stream()
                .filter(mapper -> mapper.matches(sinkRecord))
                .findFirst()
                .orElse(defaultHttpRequestMapper);

        //build HttpRequest
        HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);

        return Pair.of(sinkRecord, httpRequest);
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

    @SuppressWarnings("java:S3864")
    public List<Pair<T, HttpRequest>> prepareRequests(Collection<T> records) {
        //we submit futures to the pool
        Stream<T> stream = records.stream();
        //split SinkRecord messages, and convert them to HttpRequest
        List<Pair<T, HttpRequest>> requests = stream
                .filter(sinkRecord -> sinkRecord.value() != null)
                .peek(this::debugConnectRecord)
                .map(this::splitMessage)
                .flatMap(List::stream)
                .map(this::toHttpRequests)
                .toList();

        return groupRequests(requests);

    }

    private void debugConnectRecord(ConnectRecord<T> sinkRecord) {
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
