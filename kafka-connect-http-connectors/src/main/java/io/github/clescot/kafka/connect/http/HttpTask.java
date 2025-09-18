package io.github.clescot.kafka.connect.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.RequestTask;
import io.github.clescot.kafka.connect.http.client.*;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
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

import static io.github.clescot.kafka.connect.http.client.HttpClientFactory.buildConfigurations;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

/**
 *
 * @param <T> type of the incoming Record.
 * @param <C> client type, which is a subclass of HttpClient
 * @param <NR> native HttpRequest
 * @param <NS> native HttpResponse
 */
@SuppressWarnings({"java:S3740","java:S119"})
//we don't want to use the generic of ConnectRecord, to handle both SinkRecord and SourceRecord
//we use NR and NS to avoid confusion with the R and S of HttpClient
public class HttpTask<T,C extends HttpClient<NR, NS>, NR, NS> implements RequestTask<C,HttpConfiguration<C, NR, NS>,HttpRequest,HttpExchange> {


    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);
    


    private final Map<String,HttpConfiguration<C, NR, NS>> configurations;
    private final RetryPolicy<HttpExchange> retryPolicy;
    private Map<String,HttpConfiguration<C, NR, NS>> userConfigurations = Maps.newHashMap();
    private static CompositeMeterRegistry meterRegistry;



    private ExecutorService executorService;

    private List<RequestGrouper<T>> requestGroupers;
    private Map<String, String> settings;

    public HttpTask(HttpConnectorConfig httpConnectorConfig,
                    HttpClientFactory<C, NR, NS> httpClientFactory) {

        //build executorService
        Optional<Integer> customFixedThreadPoolSize = Optional.ofNullable(httpConnectorConfig.getInt(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE));
        customFixedThreadPoolSize.ifPresent(integer -> this.executorService = buildExecutorService(integer));

        //build meterRegistry
        settings = httpConnectorConfig.originalsStrings();
        meterRegistry = buildMeterRegistry(settings);
        bindMetrics(settings,meterRegistry, executorService);

        //request groupers
        RequestGrouperFactory requestGrouperFactory = new RequestGrouperFactory();
        this.requestGroupers = requestGrouperFactory.buildRequestGroupers(httpConnectorConfig, httpConnectorConfig.getList(REQUEST_GROUPER_IDS));
        this.retryPolicy = buildRetryPolicy(httpConnectorConfig.originalsStrings());
        //configurations
        Map<String,C> httpClientConfigurations = buildConfigurations(
                httpClientFactory,
                executorService,
                httpConnectorConfig.getConfigurationIds(),
                settings,
                meterRegistry
        );
        //wrap configurations in HttpConfiguration
        this.configurations = httpClientConfigurations.entrySet().stream()
                .map(
                        entry->Map.entry(entry.getKey(),
                        new HttpConfiguration<>(
                                entry.getKey(),
                                entry.getValue(),
                                executorService,
                                retryPolicy,
                                settings)
                        )
                )
                .collect(
                        Collectors.<Map.Entry<String, HttpConfiguration<C, NR, NS>>, String, HttpConfiguration<C, NR, NS>>toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue)
                );

    }

    /**
     * get the Configuration matching the HttpRequest, and do the Http call with a retry policy.
     * @param httpRequest http request
     * @return a future of the HttpExchange (complete request and response informations).
     */
    @Override
    public CompletableFuture<HttpExchange> call(@NotNull HttpRequest httpRequest) {
        HttpConfiguration<C, NR, NS> foundConfiguration = selectConfiguration(httpRequest);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("configuration found:{}", foundConfiguration.getId());
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
    public Map<String, HttpConfiguration<C, NR, NS>> getUserConfigurations() {
        return userConfigurations;
    }

    @Override
    public HttpConfiguration<C, NR, NS> getConfigurationForUser(String userId, HttpConfiguration<C, NR, NS> configuration) {
        HttpConfiguration<C,NR,NS> clone;
        try {
            clone = (HttpConfiguration) configuration.clone();
            C client = clone.getClient();
            C customized = (C) client.customizeForUser(userId);
            clone.setClient(customized);

        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        return clone;
    }


    @Override
    public Map<String,HttpConfiguration<C, NR, NS>> getConfigurations() {
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

    private String retryPolicyToString(){
        StringBuilder result = new StringBuilder("{");

        String retries = settings.get(RETRIES);
        if(retries!=null){
            result.append(", retries:'").append(retries).append("'");
        }
        String retryDelayInMs = settings.get(RETRY_DELAY_IN_MS);
        if(retryDelayInMs!=null){
            result.append(", retryDelayInMs:'").append(retryDelayInMs).append("'");
        }
        String maxRetryDelayInMs = settings.get(RETRY_MAX_DELAY_IN_MS);
        if(maxRetryDelayInMs!=null){
            result.append(", maxRetryDelayInMs:'").append(maxRetryDelayInMs).append("'");
        }
        String retryDelayFactor = settings.get(RETRY_DELAY_FACTOR);
        if(retryDelayFactor!=null){
            result.append(", retryDelayFactor:'").append(retryDelayFactor).append("'");
        }
        String retryjitterInMs = settings.get(RETRY_JITTER_IN_MS);
        if(retryjitterInMs!=null){
            result.append(", retryjitterInMs:'").append(retryjitterInMs).append("'");
        }
        result.append("}");
        return result.toString();
    }

    @Override
    public String toString() {
        return "HttpTask{" +
                "configurations=" + configurations +
                ", retryPolicy=" + retryPolicyToString() +
                ", userConfigurations=" + userConfigurations +
                ", executorService=" + executorService +
                ", requestGroupers=" + requestGroupers +
                ", settings=" + settings +
                '}';
    }
}
