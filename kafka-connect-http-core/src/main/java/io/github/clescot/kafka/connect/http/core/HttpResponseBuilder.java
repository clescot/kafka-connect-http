package io.github.clescot.kafka.connect.http.core;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class HttpResponseBuilder {


    private Integer statusMessageLimit = Integer.MAX_VALUE;
    private Integer bodyLimit = Integer.MAX_VALUE;
    private String protocol;
    private int statusCode;
    private String statusMessage;
    private String bodyAsString;
    private Map<String, List<String>> headers = Maps.newHashMap();


    public HttpResponseBuilder(Integer statusMessageLimit, Integer bodyLimit) {
        if(statusMessageLimit!=null) {
            this.statusMessageLimit = Math.max(0, statusMessageLimit);
        }
        if(bodyLimit!=null) {
            this.bodyLimit = Math.max(0, bodyLimit);
        }
    }

    public Integer getStatusMessageLimit() {
        return statusMessageLimit;
    }

    public Integer getBodyLimit() {
        return bodyLimit;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatus(int statusCode,String statusMessage) {
        this.statusCode = statusCode;
        if(statusMessage!=null) {
            this.statusMessage = statusMessage.substring(0,Math.min(statusMessage.length(),statusMessageLimit));
        }
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getBodyAsString() {
        return bodyAsString;
    }

    public void setBodyAsString(String bodyAsString) {
        if(bodyAsString!=null) {
            this.bodyAsString = bodyAsString.substring(0,Math.min(bodyAsString.length(),bodyLimit));
        }
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    public HttpResponse toHttpResponse(){
        HttpResponse httpResponse = new HttpResponse(statusCode,statusMessage);
        httpResponse.setProtocol(protocol);
        httpResponse.setHeaders(headers);
        httpResponse.setBodyAsString(bodyAsString);
        return httpResponse;
    }
}
