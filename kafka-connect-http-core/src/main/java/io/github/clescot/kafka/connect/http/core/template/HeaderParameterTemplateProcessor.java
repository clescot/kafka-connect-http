package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Header/Parameter template processor for accessing HTTP headers, query parameters, and cookies.
 * Provides direct access to HTTP metadata without manual parsing.
 */
public class HeaderParameterTemplateProcessor implements ExchangeTemplateProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(HeaderParameterTemplateProcessor.class);
    
    @Override
    public Exchange<?, ?> process(@NotNull Exchange<?, ?> exchange, @NotNull String template, Map<String, Object> context) {
        try {
            if (!(exchange instanceof HttpExchange)) {
                log.warn("HeaderParameter processor only works with HttpExchange, got: {}", exchange.getClass().getName());
                return exchange;
            }
            
            HttpExchange httpExchange = (HttpExchange) exchange;
            HttpRequest request = httpExchange.getRequest();
            HttpResponse response = httpExchange.getResponse();
            
            // Extract the type and name from template
            // Template format: ${header:headerName:attributeName}
            //                  ${param:paramName:attributeName}
            //                  ${cookie:cookieName:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 2) {
                log.warn("Invalid header/param template format: {}", template);
                return exchange;
            }
            
            String type = parts[0];
            String name = parts[1];
            String attributeName = parts.length > 2 ? parts[2] : type + "_" + name;
            
            String value = "";
            
            switch (type.toLowerCase()) {
                case "header":
                    value = getHeaderValue(request, response, name);
                    break;
                case "param":
                    value = getQueryParameter(request, name);
                    break;
                case "cookie":
                    value = getCookieValue(request, name);
                    break;
                default:
                    log.warn("Unknown header/param type: {}", type);
                    return exchange;
            }
            
            log.debug("{} '{}' = '{}'", type, name, value);
            return exchange.withAttribute(attributeName, value);
            
        } catch (Exception e) {
            log.warn("Failed to process header/param template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }
    
    @Override
    public boolean supports(@NotNull String template) {
        return template.startsWith("${header:") || 
               template.startsWith("${param:") || 
               template.startsWith("${cookie:");
    }
    
    @Override
    public String getName() {
        return "headerparam";
    }
    
    /**
     * Extract parts from template: ${type:name:attributeName}
     * Returns array where [0] = type, [1] = name, [2] = attributeName (if present)
     */
    private String[] extractTemplateParts(String template) {
        // Remove ${ and }
        String innerContent = template.substring(2, template.length() - 1);
        
        // Split by colons
        String[] parts = innerContent.split(":", 3);
        
        if (parts.length >= 2) {
            return parts;
        } else {
            return new String[]{parts[0], ""};
        }
    }
    
    /**
     * Get header value from request or response
     */
    private String getHeaderValue(HttpRequest request, HttpResponse response, String headerName) {
        // Try request headers first
        if (request != null && request.getHeaders() != null) {
            List<String> values = request.getHeaders().get(headerName);
            if (values != null && !values.isEmpty()) {
                return values.get(0); // Return first value
            }
        }
        
        // Try response headers
        if (response != null && response.getHeaders() != null) {
            List<String> values = response.getHeaders().get(headerName);
            if (values != null && !values.isEmpty()) {
                return values.get(0); // Return first value
            }
        }
        
        return "";
    }
    
    /**
     * Get query parameter from request URL
     */
    private String getQueryParameter(HttpRequest request, String paramName) {
        if (request == null || request.getUrl() == null) {
            return "";
        }
        
        // Parse query parameters from URL
        String url = request.getUrl();
        String queryString = "";
        
        int questionMarkIndex = url.indexOf('?');
        if (questionMarkIndex >= 0) {
            queryString = url.substring(questionMarkIndex + 1);
        }
        
        // Parse query string
        for (String param : queryString.split("&")) {
            String[] keyValue = param.split("=", 2);
            if (keyValue.length == 2 && keyValue[0].equals(paramName)) {
                return keyValue[1];
            }
        }
        
        return "";
    }
    
    /**
     * Get cookie value from request headers
     */
    private String getCookieValue(HttpRequest request, String cookieName) {
        if (request == null || request.getHeaders() == null) {
            return "";
        }
        
        List<String> cookieHeaders = request.getHeaders().get("Cookie");
        if (cookieHeaders == null || cookieHeaders.isEmpty()) {
            return "";
        }
        
        String cookieHeader = cookieHeaders.get(0); // Get first cookie header
        if (cookieHeader == null || cookieHeader.isEmpty()) {
            return "";
        }
        
        // Parse cookies
        for (String cookie : cookieHeader.split(";")) {
            String[] keyValue = cookie.split("=", 2);
            if (keyValue.length == 2 && keyValue[0].trim().equals(cookieName)) {
                return keyValue[1].trim();
            }
        }
        
        return "";
    }
}