package io.github.clescot.kafka.connect;


/**
 * Interface for a configuration that matches a request and provides a client.
 *
 * @param <C> the type of the client
 * @param <R> the type of the request
 */
public interface Configuration<C extends Client, R> {
    /**
     * Default configuration ID used when no specific configuration is provided.
     */
    String DEFAULT_CONFIGURATION_ID = "default";

    /**
     * Checks if the configuration matches the given request.
     *
     * @param request the request to check
     * @return true if the configuration matches the request, false otherwise
     */
    boolean matches(R request);

    /**
     * Gets the ID of the configuration.
     *
     * @return the configuration ID
     */
    String getId();

    /**
     * Gets the client associated with this configuration.
     *
     * @return the client
     */
    C getClient();
}
