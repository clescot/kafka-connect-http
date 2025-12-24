package io.github.clescot.kafka.connect;

import com.google.common.base.Preconditions;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.http.client.RetryException;
import io.github.clescot.kafka.connect.http.core.Request;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.github.clescot.kafka.connect.http.core.Request.VU_ID;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

/**
 * A task that handles requests and produces responses.
 * 
 * @param <C> the type of Client
 * @param <F> the type of Configuration
 * @param <R> the type of Request
 * @param <E> the type of Exchange/Response
 */
public interface RequestTask<C extends Client<E>, F extends Configuration<C, R>, R extends Request, E>
        extends Task<C, F, R> {

    String HAS_BEEN_SET = " has been set.";

    String MUST_BE_SET_TOO = " must be set too.";

    /**
     * Calls the service with the provided request.
     * 
     * @param request the request to be sent
     * @return a CompletableFuture representing the asynchronous operation,
     *         containing the response
     */
    CompletableFuture<E> call(@NotNull R request);

    /**
     * Returns a map of user-specific configurations.
     * The key is a combination of user ID and configuration ID.
     * 
     * @return a map of user-specific configurations
     */
    Map<String, F> getUserConfigurations();

    /**
     * Allows to customize a configuration for a specific user.
     * By default, it returns the provided configuration without any modification.
     * 
     * @param userId        the ID of the user
     * @param configuration the configuration to be customized
     * @return the customized configuration
     */

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
        Map<String, F> configurations = getConfigurations();
        Preconditions.checkArgument(!configurations.isEmpty(), "Configurations list must not be null or empty.");
        String vuId = Optional.ofNullable(request.getAttributes().get(VU_ID)).map(Object::toString)
                .orElse(Request.DEFAULT_VU_ID);
        // is there a matching configuration against the request ?
        F configuration = getDefaultConfiguration();
        F matchingConfiguration = configurations
                .values().stream()
                .filter(config -> config.matches(request))
                .findFirst().orElse(configuration);// default configuration
        String configurationId = matchingConfiguration.getId();
        String configurationForUserId = vuId + "-" + configurationId;
        F configurationForUser;
        if (getUserConfigurations().containsKey(configurationForUserId)) {
            configurationForUser = getUserConfigurations().get(configurationForUserId);
        } else {
            configurationForUser = getConfigurationForUser(vuId, matchingConfiguration);
            getUserConfigurations().put(configurationForUserId, configurationForUser);
        }
        return configurationForUser;
    }

    default RetryPolicy<E> buildRetryPolicy(Map<String, String> settings) {
        RetryPolicy<E> retryPolicy = null;
        if (settings.containsKey(RETRIES)) {
            Integer retries = Integer.parseInt(settings.get(RETRIES));
            Long retryDelayInMs = Long.parseLong(
                    Optional.ofNullable(settings.get(RETRY_DELAY_IN_MS)).orElse("" + DEFAULT_RETRY_DELAY_IN_MS_VALUE));
            Long retryMaxDelayInMs = Long.parseLong(Optional.ofNullable(settings.get(RETRY_MAX_DELAY_IN_MS))
                    .orElse("" + DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE));
            Double retryDelayFactor = Double.parseDouble(Optional.ofNullable(settings.get(RETRY_DELAY_FACTOR))
                    .orElse("" + DEFAULT_RETRY_DELAY_FACTOR_VALUE));
            Long retryJitterInMs = Long.parseLong(Optional.ofNullable(settings.get(RETRY_JITTER_IN_MS))
                    .orElse("" + DEFAULT_RETRY_JITTER_IN_MS_VALUE));
            retryPolicy = buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor,
                    retryJitterInMs);
        } else {
            LOGGER.trace("retry policy is not configured");
        }
        return retryPolicy;
    }

    private RetryPolicy<E> buildRetryPolicy(Integer retries,
            Long retryDelayInMs,
            Long retryMaxDelayInMs,
            Double retryDelayFactor,
            Long retryJitterInMs) {
        // noinspection LoggingPlaceholderCountMatchesArgumentCount
        return RetryPolicy.<E>builder()
                // we retry only if the error comes from the WS server (server-side technical
                // error)
                .handle(RetryException.class)
                .withBackoff(Duration.ofMillis(retryDelayInMs), Duration.ofMillis(retryMaxDelayInMs), retryDelayFactor)
                .withJitter(Duration.ofMillis(retryJitterInMs))
                .withMaxRetries(retries)
                .onAbort(listener -> LOGGER.warn("Retry  aborted ! result:'{}', failure:'{}'", listener.getResult(),
                        listener.getException()))
                .onRetriesExceeded(listener -> LOGGER.warn(
                        "Retries exceeded  elapsed attempt time:'{}', attempts:'{}', call result:'{}', failure:'{}'",
                        listener.getElapsedAttemptTime(), listener.getAttemptCount(), listener.getResult(),
                        listener.getException()))
                .onRetry(listener -> LOGGER.trace("Retry  call result:'{}', failure:'{}'", listener.getLastResult(),
                        listener.getLastException()))
                .onFailure(listener -> LOGGER.warn("call failed ! result:'{}',exception:'{}'", listener.getResult(),
                        listener.getException()))
                .build();
    }

}
