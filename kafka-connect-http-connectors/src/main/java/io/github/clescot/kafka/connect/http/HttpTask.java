package io.github.clescot.kafka.connect.http;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

/**
 *
 * @param <T> either a SinkRecord (for a SinkTask) or a SourceRecord (for a SourceTask)
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 */
public class HttpTask<T extends ConnectRecord<T>, R, S> {


    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTask.class);


    private final List<Configuration<R, S>> customConfigurations;
    private final Configuration<R, S> defaultConfiguration;
    private final ExecutorService executorService;
    private static CompositeMeterRegistry meterRegistry;


    public HttpTask(AbstractConfig config,
                    Configuration<R, S> defaultConfiguration,
                    List<Configuration<R, S>> customConfigurations,
                    CompositeMeterRegistry meterRegistry,
                    ExecutorService executorService) {

        this.executorService = executorService;
        if (HttpTask.meterRegistry == null) {
            HttpTask.meterRegistry = meterRegistry;
        }
        //bind metrics to MeterRegistry and ExecutorService
        bindMetrics(config, meterRegistry, executorService);
        this.defaultConfiguration = defaultConfiguration;
        this.customConfigurations = customConfigurations;
    }

    /**
     * get the Configuration matching the HttpRequest, and do the Http call with a retry policy.
     * @param httpRequest
     * @return
     */
    public CompletableFuture<HttpExchange> processHttpRequest(HttpRequest httpRequest) {
        Configuration<R, S> foundConfiguration = getConfiguration(httpRequest);
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

    private Configuration<R, S> getConfiguration(HttpRequest httpRequest) {
        //is there a matching configuration against the request ?
        return customConfigurations
                .stream()
                .filter(config -> config.matches(httpRequest))
                .findFirst()
                .orElse(defaultConfiguration);
    }

    private static void bindMetrics(AbstractConfig config, MeterRegistry meterRegistry, ExecutorService myExecutorService) {
        boolean bindExecutorServiceMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE));
        if (bindExecutorServiceMetrics) {
            new ExecutorServiceMetrics(myExecutorService, "HttpSinkTask", Lists.newArrayList()).bindTo(meterRegistry);
        }
        boolean bindJvmMemoryMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_MEMORY));
        if (bindJvmMemoryMetrics) {
            new JvmMemoryMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmThreadMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_THREAD));
        if (bindJvmThreadMetrics) {
            new JvmThreadMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmInfoMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_INFO));
        if (bindJvmInfoMetrics) {
            new JvmInfoMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmGcMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_GC));
        if (bindJvmGcMetrics) {
            try (JvmGcMetrics gcMetrics = new JvmGcMetrics()) {
                gcMetrics.bindTo(meterRegistry);
            }
        }
        boolean bindJVMClassLoaderMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER));
        if (bindJVMClassLoaderMetrics) {
            new ClassLoaderMetrics().bindTo(meterRegistry);
        }
        boolean bindJVMProcessorMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR));
        if (bindJVMProcessorMetrics) {
            new ProcessorMetrics().bindTo(meterRegistry);
        }
        boolean bindLogbackMetrics = Boolean.parseBoolean(config.getString(METER_REGISTRY_BIND_METRICS_LOGBACK));
        if (bindLogbackMetrics) {
            try (LogbackMetrics logbackMetrics = new LogbackMetrics()) {
                logbackMetrics.bindTo(meterRegistry);
            }
        }
    }


    public Configuration<R, S> getDefaultConfiguration() {
        return defaultConfiguration;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public static CompositeMeterRegistry getMeterRegistry() {
        return HttpTask.meterRegistry;
    }

    public static void removeCompositeMeterRegistry() {
        HttpTask.meterRegistry = null;
    }
}
