package io.github.clescot.kafka.connect;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.MeterRegistryFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.METER_REGISTRY_BIND_METRICS_JVM_GC;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.METER_REGISTRY_BIND_METRICS_JVM_INFO;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.METER_REGISTRY_BIND_METRICS_LOGBACK;

/**
 * Task interface for handling requests.
 * This interface defines methods for selecting configurations based on requests.
 *
 * @param <C> the type of client used to make requests
 * @param <F> the type of the configuration for the client
 * @param <R> the type of request
 * @param <S> the type of response
 */
public interface Task<C extends Client,F extends Configuration<C,R>,R,S> {


    /**
     * Selects a configuration based on the provided request.
     * If no matching configuration is found, it returns the default configuration.
     *
     * @param request the request to match against configurations
     * @return the selected configuration
     */
    default F selectConfiguration(R request) {
        Preconditions.checkNotNull(request, "Request must not be null.");
        Map<String,F> configurations = getConfigurations();
        Preconditions.checkArgument(!configurations.isEmpty(), "Configurations list must not be null or empty.");
        //is there a matching configuration against the request ?
        F configuration = configurations.get(0);
        return configurations
                .values().stream()
                .filter(config -> config.matches(request))
                .findFirst().orElse(configuration); //default configuration
    }

    Map<String,F> getConfigurations();


    default F getDefaultConfiguration() {
        Map<String,F> configurations = getConfigurations();
        Preconditions.checkArgument(!configurations.isEmpty(), "Configurations list must not be null or empty.");
        //return the first configuration as default
        return configurations.get(Configuration.DEFAULT_CONFIGURATION_ID);
    }

    default CompositeMeterRegistry buildMeterRegistry(Map<String,String> settings) {
        MeterRegistryFactory meterRegistryFactory = new MeterRegistryFactory();
        return meterRegistryFactory.buildMeterRegistry(settings);
    }

    default void bindMetrics(Map<String,String> config, MeterRegistry meterRegistry, ExecutorService myExecutorService) {
        boolean bindExecutorServiceMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE));

        //executorService metrics
        if (bindExecutorServiceMetrics && myExecutorService != null) {
            new ExecutorServiceMetrics(myExecutorService, "HttpSinkTask", Lists.newArrayList()).bindTo(meterRegistry);
        }

        //jvm metrics
        boolean bindJvmMemoryMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_MEMORY));
        if (bindJvmMemoryMetrics) {
            new JvmMemoryMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmThreadMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_THREAD));
        if (bindJvmThreadMetrics) {
            new JvmThreadMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmInfoMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_INFO));
        if (bindJvmInfoMetrics) {
            new JvmInfoMetrics().bindTo(meterRegistry);
        }
        boolean bindJvmGcMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_GC));
        if (bindJvmGcMetrics) {
            try (JvmGcMetrics gcMetrics = new JvmGcMetrics()) {
                gcMetrics.bindTo(meterRegistry);
            }
        }
        boolean bindJVMClassLoaderMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER));
        if (bindJVMClassLoaderMetrics) {
            new ClassLoaderMetrics().bindTo(meterRegistry);
        }
        boolean bindJVMProcessorMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR));
        if (bindJVMProcessorMetrics) {
            new ProcessorMetrics().bindTo(meterRegistry);
        }

        //logging metrics
        boolean bindLogbackMetrics = Boolean.parseBoolean(config.get(METER_REGISTRY_BIND_METRICS_LOGBACK));
        if (bindLogbackMetrics) {
            try (LogbackMetrics logbackMetrics = new LogbackMetrics()) {
                logbackMetrics.bindTo(meterRegistry);
            }
        }
    }


    // This class is a placeholder for the Task class.
    // It can be extended to implement specific task functionality.

    // The generic types R and S can be used to represent request and response types respectively.
    // This allows for flexibility in defining the types of requests and responses handled by the task.

    // Additional methods and properties can be added as needed to implement specific task behavior.
}
