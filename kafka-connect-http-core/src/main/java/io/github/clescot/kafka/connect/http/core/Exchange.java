package io.github.clescot.kafka.connect.http.core;

import java.util.Map;

/**
 * Exchange interface representing a communication exchange between client and server.
 * Provides template support methods for customizing exchange output.
 */
public interface Exchange<R extends Request,S extends Response> {

    /**
     * Get all attributes associated with this exchange.
     * 
     * @return map of attribute names to values
     */
    Map<String, Object> getAttributes();

    /**
     * Get the request part of this exchange.
     * 
     * @return the request
     */
    R getRequest();

    /**
     * Get the response part of this exchange.
     * 
     * @return the response
     */
    S getResponse();

    /**
     * Get the content of this exchange as a string.
     * Used for template processing to extract data from the exchange.
     * 
     * @return the content as a string, typically from request or response body
     */
    String getContentAsString();

    /**
     * Get metadata about this exchange for template processing.
     * Includes timing information, status, and other exchange properties.
     * 
     * @return map of metadata properties
     */
    Map<String, Object> getMetadata();

    /**
     * Get a specific attribute by name.
     * 
     * @param name the attribute name
     * @return the attribute value, or null if not found
     */
    Object getAttribute(String name);

    /**
     * Create a new exchange with an additional attribute.
     * This method should return a new instance rather than modifying the current one.
     * 
     * @param name the attribute name
     * @param value the attribute value
     * @return a new exchange with the additional attribute
     */
    <T extends Exchange<R, S>> T withAttribute(String name, Object value);

}
