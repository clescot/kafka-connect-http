package io.github.clescot.kafka.connect;

import com.google.common.base.Preconditions;

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

    // This class is a placeholder for the Task class in the Kafka Connect framework.
    // It can be extended to implement specific task functionality for HTTP connectors.

    // The generic types R and S can be used to represent request and response types respectively.
    // This allows for flexibility in defining the types of requests and responses handled by the task.

    // Additional methods and properties can be added as needed to implement specific task behavior.
}
