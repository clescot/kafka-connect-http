package io.github.clescot.kafka.connect.http.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HttpResponseBuilder {


    private Integer statusMessageLimit = Integer.MAX_VALUE;
    private Integer headersLimit = Integer.MAX_VALUE;
    private Integer bodyLimit = Integer.MAX_VALUE;
    private String protocol;
    private int statusCode;
    private String statusMessage;
    private String bodyAsString;
    private Map<String, List<String>> headers = Maps.newHashMap();


    public HttpResponseBuilder(Integer statusMessageLimit, Integer headersLimit,Integer bodyLimit) {
        if(headersLimit!=null) {
            this.headersLimit = Math.max(0, headersLimit);
        }

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

    public Integer getHeadersLimit() {
        return headersLimit;
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
        Map<String, List<String>> headersWithLimit = Maps.newHashMap();
        if(headers!=null){
            int headersSize=0;
            Iterator<Map.Entry<String, List<String>>> iterator = headers.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry<String, List<String>> next = iterator.next();
                headersSize+=next.getKey().length();
                Iterator<String> valuesIterator = next.getValue().iterator();
                List<String> valuesWithLimit = Lists.newArrayList();
                while(valuesIterator.hasNext()){
                    String myValue = valuesIterator.next();
                    headersSize+=myValue.length();
                    if(headersSize<headersLimit){
                        valuesWithLimit.add(myValue);
                    }else{
                        break;
                    }
                }
                if(headersSize<headersLimit){
                    headersWithLimit.put(next.getKey(),valuesWithLimit);
                }else {
                    break;
                }
            }
        }
        this.headers = headersWithLimit;
    }

    public HttpResponse toHttpResponse(){
        HttpResponse httpResponse = new HttpResponse(statusCode,statusMessage);
        httpResponse.setProtocol(protocol);
        httpResponse.setHeaders(headers);
        httpResponse.setBodyAsString(bodyAsString);
        return httpResponse;
    }
}
