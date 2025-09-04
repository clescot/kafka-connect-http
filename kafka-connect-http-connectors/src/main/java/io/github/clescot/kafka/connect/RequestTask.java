package io.github.clescot.kafka.connect;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.core.Request;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.github.clescot.kafka.connect.http.core.Request.VU_ID;

public interface RequestTask<C extends Client,F extends Configuration<C,R>,R extends Request,E> extends Task<C,F,R>{


    CompletableFuture<E> call(@NotNull R request);


    Map<String,F> getUserConfigurations();



    default F getConfigurationForUser(String userId, F configuration) {
        return configuration;
    }

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
        String vuId = Optional.ofNullable(request.getAttributes().get(VU_ID)).orElse(Request.DEFAULT_VU_ID);
        //is there a matching configuration against the request ?
        F configuration = getDefaultConfiguration();
        F matchingConfiguration = configurations
                .values().stream()
                .filter(config -> config.matches(request))
                .findFirst().orElse(configuration);//default configuration
        String configurationId = matchingConfiguration.getId();
        String configurationForUserId = vuId+"-"+configurationId;
        if(getUserConfigurations().containsKey(configurationForUserId)){
            return getUserConfigurations().get(configurationForUserId);
        }else{
            getUserConfigurations().put(configurationForUserId,getConfigurationForUser(vuId,matchingConfiguration));
        }
        return matchingConfiguration;
    }

}
