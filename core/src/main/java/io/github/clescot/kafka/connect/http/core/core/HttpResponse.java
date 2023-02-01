package io.github.clescot.kafka.connect.http.core.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpResponse.SCHEMA_AS_STRING,
        refs = {})
public class HttpResponse implements Serializable {

    public static final long serialVersionUID = 1L;
    public static final String SCHEMA_AS_STRING = "{\n" +
            "  \"$id\": \"https://github.com/clescot/kafka-connect-http-sink/schemas/http-response.json\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema\",\n" +
            "  \"title\": \"Http Response schema.\",\n" +
            "  \"description\": \"Http Response schema, included into HttpExchange.\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"javaType\" : \"com.github.clescot.kafka.connect.http.HttpResponse\",\n" +
            "  \"additionalProperties\": false,\n" +
            "  \"properties\": {\n" +
            "    \"statusCode\":{\n" +
            "      \"type\": \"integer\"\n" +
            "    },\n" +
            "    \"statusMessage\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"protocol\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"responseHeaders\":  {\n" +
            "      \"type\": \"object\",\n" +
            "      \"existingJavaType\" : \"java.util.Map<String,java.util.List<String>>\",\n" +
            "      \"additionalProperties\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"string\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"responseBody\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"statusCode\",\n" +
            "    \"statusMessage\"\n" +
            "  ]\n" +
            "}";

    private Integer statusCode;
    private String statusMessage;
    private String responseBody;
    private String protocol;

    private Map<String, List<String>> responseHeaders = Maps.newHashMap();

    /**
     * only for json deserialization
     */
    protected HttpResponse() {
    }

    public HttpResponse(Integer statusCode, String statusMessage) {
        Preconditions.checkArgument(statusCode>0,"status code must be a positive integer");
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
    }

    public Map<String, List<String>> getResponseHeaders() {
        return responseHeaders;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public void setResponseHeaders(Map<String, List<String>> responseHeaders) {
        this.responseHeaders = responseHeaders;
    }

    public void setResponseBody(String responseBody) {
        this.responseBody = responseBody;
    }


    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    protected void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    protected void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpResponse that = (HttpResponse) o;
        return statusCode.equals(that.statusCode) && statusMessage.equals(that.statusMessage) && protocol.equals(that.protocol)&& responseBody.equals(that.responseBody) && Objects.equals(responseHeaders, that.responseHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusCode, statusMessage, responseBody, responseHeaders);
    }

    @Override
    public String toString() {
        return "HttpResponse{" +
                "statusCode=" + statusCode +
                ", statusMessage='" + statusMessage + '\'' +
                ", protocol='" + protocol + '\'' +
                ", responseBody='" + responseBody + '\'' +
                ", responseHeaders=" + responseHeaders +
                '}';
    }
}
