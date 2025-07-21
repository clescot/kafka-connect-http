package io.github.clescot.kafka.connect.http.client;


/**
 * Interface for a configuration that matches a request and provides a client.
 *
 * @param <C> the type of the client
 * @param <R> the type of the request
 */
public interface Configuration<C,R> {
    String DEFAULT_CONFIGURATION_ID = "default";
    boolean matches(R request);

    C getClient();
}
