package io.github.clescot.kafka.connect.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.Task;
import io.github.clescot.kafka.connect.http.client.*;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigurationFactory.buildConfigurations;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.REQUEST_GROUPER_IDS;

/**
 * @param <T> type of the incoming Record.
 * @param <C> client type, which is a subclass of HttpClient
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 */
@SuppressWarnings("java:S3740")//we don't want to use the generic of ConnectRecord, to handle both SinkRecord and SourceRecord
public class HttpTask<T,C extends HttpClient<R,S>,R, S> implements Task<C,HttpConfiguration<C,R,S>,HttpRequest, HttpResponse> {


    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);
    


    private final Map<String,HttpConfiguration<C,R, S>> configurations;
    private static CompositeMeterRegistry meterRegistry;



    private ExecutorService executorService;

    private List<RequestGrouper<T>> requestGroupers;

    public HttpTask(HttpConnectorConfig httpConnectorConfig,
                    HttpClientFactory<C, R, S> httpClientFactory) {

        //build executorService
        Optional<Integer> customFixedThreadPoolSize = Optional.ofNullable(httpConnectorConfig.getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE));
        customFixedThreadPoolSize.ifPresent(integer -> this.executorService = buildExecutorService(integer));

        //build meterRegistry
        Map<String, String> originalsStrings = httpConnectorConfig.originalsStrings();
        meterRegistry = buildMeterRegistry(originalsStrings);
        bindMetrics(originalsStrings,meterRegistry, executorService);

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

    /**
     * Group the requests using the requestGroupers.
     * @param pairList
     * @return
     */
    public List<Pair<T, HttpRequest>> groupRequests(List<Pair<T, HttpRequest>> pairList) {
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


    public static synchronized void clearMeterRegistry() {
        meterRegistry = null;
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
                throw new HttpException(e);
            }
            LOGGER.info("executor is shutdown : '{}'", executorService.isShutdown());
            LOGGER.info("executor tasks are terminated : '{}'", executorService.isTerminated());
        }
        if (meterRegistry != null) {
            meterRegistry.close();
        }
        LOGGER.info("HttpTask stopped");
    }

}
