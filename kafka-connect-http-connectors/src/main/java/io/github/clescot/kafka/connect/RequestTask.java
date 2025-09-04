package io.github.clescot.kafka.connect;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.core.Request;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface RequestTask<C extends Client,F extends Configuration<C,R>,R extends Request,E> extends Task<C,F,R>{


    CompletableFuture<E> call(@NotNull R request);




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
        F configuration = getDefaultConfiguration();
        return configurations
                .values().stream()
                .filter(config -> config.matches(request))
                .findFirst().orElse(configuration); //default configuration
    }

}
