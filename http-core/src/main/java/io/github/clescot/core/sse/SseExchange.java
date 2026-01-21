package io.github.clescot.core.sse;

import io.github.clescot.core.http.Exchange;
import io.github.clescot.core.http.HttpRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents an SSE Exchange that wraps an SseEvent for template processing.
 * This allows SSE events to be processed using the same template system as HTTP exchanges.
 */
public class SseExchange implements Exchange<HttpRequest, SseEvent> {
    
    private final HttpRequest request;
    private SseEvent response;
    private final Map<String, Object> attributes;
    private final Map<String, Object> context;
    
    /**
     * Creates a new SseExchange.
     *
     * @param request the HTTP request that initiated the SSE connection
     * @param response the SSE event response
     * @param context additional context for template processing
     */
    public SseExchange(HttpRequest request, SseEvent response, Map<String, Object> context) {
        this.request = request;
        this.response = response;
        this.attributes = new HashMap<>();
        this.context = context;
    }
    
    /**
     * Creates a new SseExchange with empty context.
     *
     * @param request the HTTP request that initiated the SSE connection
     * @param response the SSE event response
     */
    public SseExchange(HttpRequest request, SseEvent response) {
        this(request, response, Map.of());
    }
    
    @Override
    public HttpRequest getRequest() {
        return request;
    }
    
    @Override
    public SseEvent getResponse() {
        return response;
    }
    
    @Override
    public Map<String, Object> getAttributes() {
        return Map.copyOf(attributes);
    }
    
    @Override
    public String getContent() {
        return response != null ? response.getData() : "";
    }
    
    @Override
    public Map<String, Object> getMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        if (response != null) {
            metadata.put("eventId", response.getId());
            metadata.put("eventType", response.getType());
            metadata.put("success", response.isSuccess());
        }
        if (request != null) {
            metadata.put("requestUrl", request.getUrl());
            metadata.put("requestMethod", request.getMethod());
        }
        return metadata;
    }
    
    @Override
    public Object getAttribute(String name) {
        return attributes.get(name);
    }
    
    @Override
    public Exchange<HttpRequest, SseEvent> withAttribute(String name, Object value) {
        SseExchange newExchange = new SseExchange(this.request, this.response, this.context);
        newExchange.attributes.putAll(this.attributes);
        newExchange.attributes.put(name, value);
        return newExchange;
    }
    
    /**
     * Gets the additional context for template processing.
     *
     * @return the context map
     */
    public Map<String, Object> getContext() {
        return context;
    }
    
    /**
     * Checks if this exchange is successful.
     * SSE events are always considered successful since they represent received events.
     *
     * @return always true for SSE events
     */
    public boolean isSuccess() {
        return response != null && response.isSuccess();
    }
    
    /**
     * Creates a new SseExchange with the processed response.
     *
     * @param processedResponse the processed SSE event
     * @return a new SseExchange with the processed response
     */
    public SseExchange withResponse(SseEvent processedResponse) {
        return new SseExchange(this.request, processedResponse, this.context);
    }
    
    /**
     * Creates a new SseExchange with additional context.
     *
     * @param additionalContext additional context to merge with existing context
     * @return a new SseExchange with merged context
     */
    public SseExchange withContext(Map<String, Object> additionalContext) {
        Map<String, Object> mergedContext = Map.copyOf(this.context);
        mergedContext.putAll(additionalContext);
        return new SseExchange(this.request, this.response, mergedContext);
    }

    /**
     * Creates a new SseExchange with the content set.
     * Updates the response data to the new content.
     * Preserves attributes from the original response event.
     *
     * @param content the new content to set
     * @return a new SseExchange with the updated response
     */
    @Override
    public SseExchange setContent(String content) {
        if (response == null) {
            return this;
        }
        this.response = new SseEvent(response.getId(), response.getType(), content);
        this.response.getAttributes().putAll(response.getAttributes());
        SseExchange newExchange = new SseExchange(this.request, this.response, this.context);
        newExchange.attributes.putAll(this.attributes);
        return newExchange;
    }
}