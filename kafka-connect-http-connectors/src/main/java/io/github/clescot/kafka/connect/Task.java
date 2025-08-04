package io.github.clescot.kafka.connect;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.MeterRegistryFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.util.Map;

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

    // This class is a placeholder for the Task class.
    // It can be extended to implement specific task functionality.

    // The generic types R and S can be used to represent request and response types respectively.
    // This allows for flexibility in defining the types of requests and responses handled by the task.

    // Additional methods and properties can be added as needed to implement specific task behavior.
}
