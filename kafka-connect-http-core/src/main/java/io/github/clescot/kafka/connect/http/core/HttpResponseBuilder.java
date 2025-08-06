package io.github.clescot.kafka.connect.http.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.github.clescot.kafka.connect.http.core.MediaType.APPLICATION_OCTET_STREAM;
import static io.github.clescot.kafka.connect.http.core.MediaType.APPLICATION_X_WWW_FORM_URLENCODED;

public class HttpResponseBuilder {


    private Integer statusMessageLimit = Integer.MAX_VALUE;
    private Integer headersLimit = Integer.MAX_VALUE;
    private Integer bodyLimit = Integer.MAX_VALUE;
    private String protocol;
    private int statusCode;
    private String statusMessage;
    private String bodyAsString;
    private BodyType bodyType = BodyType.STRING;
    private String bodyAsByteArray;
    private Map<String, List<String>> headers = Maps.newHashMap();
    private Map<String, String> bodyAsForm = Maps.newHashMap();

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


    public byte[] getBodyAsByteArray() {
        if (bodyAsByteArray != null && !bodyAsByteArray.isEmpty()) {
            return Base64.getDecoder().decode(bodyAsByteArray);
        }
        return null;
    }


    public String getBodyAsString() {
        return bodyAsString;
    }

    public void setContentType(String contentType) {
        if (headers == null) {
            headers = Maps.newHashMap();
        }
        if (contentType != null && !contentType.isEmpty()) {
            List<String> contentTypes = Lists.newArrayList(contentType);
            headers.put(MediaType.KEY, contentTypes);
        } else {
            headers.remove(MediaType.KEY);
        }
    }

    public String getContentType() {
        if (headers != null && headers.containsKey(MediaType.KEY)) {
            List<String> contentTypes = headers.get(MediaType.KEY);
            if (contentTypes != null && !contentTypes.isEmpty()) {
                return contentTypes.get(0);
            }
        }
        return null;
    }

    public void setBodyAsString(String bodyAsString) {
        if(bodyAsString!=null) {
            this.bodyAsString = bodyAsString.substring(0,Math.min(bodyAsString.length(),bodyLimit));
            this.bodyType = BodyType.STRING;
        }
    }

    public void setBodyAsByteArray(byte[] content) {
        Preconditions.checkNotNull(content, "bodyAsByteArray cannot be null");
        Preconditions.checkArgument(bodyLimit>=content.length,"bodyAsByteArray length exceeds bodyLimit");

        if (content != null && content.length > 0) {
            bodyAsByteArray = Base64.getEncoder().encodeToString(content);
            bodyType = BodyType.BYTE_ARRAY;

            //if no Content-Type is set, we set the default application/octet-stream
            if (headers != null && doesNotContainHeader(MediaType.KEY)) {
                headers.put(MediaType.KEY, Lists.newArrayList(APPLICATION_OCTET_STREAM));
            }
        }

    }

    private boolean doesNotContainHeader(String key) {
        return headers.keySet().stream().filter(k -> k.equalsIgnoreCase(key)).findAny().isEmpty();
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
                int keyLength = next.getKey().length();
                if(headersSize+keyLength<headersLimit) {
                    headersSize += keyLength;
                    Iterator<String> valuesIterator = next.getValue().iterator();
                    List<String> valuesWithLimit = Lists.newArrayList();
                    while (valuesIterator.hasNext()) {
                        String myValue = valuesIterator.next();
                        if (headersSize + myValue.length() < headersLimit) {
                            headersSize += myValue.length();
                            valuesWithLimit.add(myValue);
                        } else {
                            break;
                        }
                    }
                    if (headersSize < headersLimit) {
                        headersWithLimit.put(next.getKey(), valuesWithLimit);
                    } else {
                        break;
                    }
                }
            }
        }
        this.headers = headersWithLimit;
    }

    public void setBodyType(BodyType bodyType) {
        Preconditions.checkNotNull(bodyType, "bodyType cannot be null");
        this.bodyType = bodyType;
    }

    public HttpResponse toHttpResponse(){
        HttpResponse httpResponse = new HttpResponse(statusCode,statusMessage);
        httpResponse.setProtocol(protocol);
        httpResponse.setHeaders(headers);
        httpResponse.setBodyAsString(bodyAsString);
        httpResponse.setBodyAsByteArray(this.getBodyAsByteArray());
        httpResponse.setBodyAsForm(this.getBodyAsForm());
        return httpResponse;
    }

    public void setBodyAsForm(Map<String, String> form) {
        this.bodyAsForm = form;
        bodyType = BodyType.FORM;
        if (form != null && !form.isEmpty() && headers != null && doesNotContainHeader(MediaType.KEY)) {
            headers.put(MediaType.KEY, Lists.newArrayList(APPLICATION_X_WWW_FORM_URLENCODED));
        }
    }

    public Map<String, String> getBodyAsForm() {
        return bodyAsForm;
    }
}
