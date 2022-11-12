package com.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@io.confluent.kafka.schemaregistry.annotations.Schema(value = "{\n" +
        "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema#\",\n" +
        "  \"title\": \"Http Request\",\n" +
        "  \"type\": \"object\",\n" +
        "  \"additionalProperties\": false,\n" +
        "  \"required\": [\"url\",\"method\"],\n" +
        "  \"properties\": {\n" +
        "    \"requestId\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"string\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"correlationId\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"string\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"timeoutInMs\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"integer\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"retries\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"integer\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"retryDelayInMs\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"integer\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"retryMaxDelayInMs\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"integer\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"retryDelayFactor\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"number\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"retryJitter\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"integer\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"url\": {\n" +
        "      \"type\": \"string\"\n" +
        "    },\n" +
        "    \"headers\": {\n" +
        "      \"type\": \"object\",\n" +
        "      \"connect.type\": \"map\",\n" +
        "      \"additionalProperties\": {\n" +
        "        \"type\": \"array\",\n" +
        "        \"items\": {\n" +
        "          \"type\": \"string\"\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    \"method\": {\n" +
        "      \"type\": \"string\"\n" +
        "    },\n" +
        "    \"bodyAsString\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"string\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"bodyAsByteArray\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"string\"\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"bodyAsMultipart\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"array\",\n" +
        "          \"items\": {\n" +
        "            \"type\": \"string\"\n" +
        "          }\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    \"bodyType\": {\n" +
        "      \"oneOf\": [\n" +
        "        {\n" +
        "          \"type\": \"null\",\n" +
        "          \"title\": \"Not included\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"string\",\n" +
        "          \"enum\": [\n" +
        "            \"STRING\",\n" +
        "            \"BYTE_ARRAY\",\n" +
        "            \"MULTIPART\"\n" +
        "          ]\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  },\n" +
        "  \"required\": [\n" +
        "    \"url\",\n" +
        "    \"method\"\n" +
        "  ]\n" +
        "}",
        refs = {})
public class HttpRequest {


    public static final String REQUEST_ID = "requestId";
    public static final String CORRELATION_ID = "correlationId";
    public static final String TIMEOUT_IN_MS = "timeoutInMs";

    public static final String RETRIES = "retries";
    public static final String RETRY_DELAY_IN_MS = "retryDelayInMs";
    public static final String RETRY_MAX_DELAY_IN_MS = "retryMaxDelayInMs";
    public static final String RETRY_DELAY_FACTOR = "retryDelayFactor";
    public static final String RETRY_JITTER = "retryJitter";
    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";

    //only one 'body' field must be set
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";
    public static final String BODY_AS_MULTIPART = "bodyAsMultipart";
    public static final int VERSION = 1;

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpRequest.class);

    //metadata
    @JsonProperty
    private String requestId;
    @JsonProperty
    private String correlationId;

    //connection
    @JsonProperty
    private Long timeoutInMs;

    //retry policy
    @JsonProperty
    private Long retries;
    @JsonProperty
    private Long retryDelayInMs;
    @JsonProperty
    private Long retryMaxDelayInMs;
    @JsonProperty
    private Double retryDelayFactor;
    @JsonProperty
    private Long retryJitter;

    //request
    @JsonProperty(required = true)
    private String url;
    @JsonProperty
    private Map<String, List<String>> headers = Maps.newHashMap();
    @JsonProperty(required = true)
    private String method;
    @JsonProperty
    private String bodyAsString="";
    @JsonProperty
    private String bodyAsByteArray="";
    @JsonProperty
    private List<String> bodyAsMultipart = Lists.newArrayList();
    @JsonProperty
    private BodyType bodyType;


    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpRequest.class.getName())
            .version(VERSION)
            //meta-data outside of the request
            .field(REQUEST_ID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CORRELATION_ID, Schema.OPTIONAL_STRING_SCHEMA)
            //connection (override the default one set in the Sink Connector)
            .field(TIMEOUT_IN_MS, Schema.OPTIONAL_INT64_SCHEMA)
            //retry policy (override the default one set in the Sink Connector)
            .field(RETRIES, Schema.OPTIONAL_INT32_SCHEMA)
            .field(RETRY_DELAY_IN_MS, Schema.OPTIONAL_INT64_SCHEMA)
            .field(RETRY_MAX_DELAY_IN_MS, Schema.OPTIONAL_INT64_SCHEMA)
            .field(RETRY_DELAY_FACTOR, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field(RETRY_JITTER, Schema.OPTIONAL_INT64_SCHEMA)
            //request
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(URL, Schema.STRING_SCHEMA)
            .field(METHOD, Schema.STRING_SCHEMA)
            .field(BODY_TYPE, Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_MULTIPART, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA));

    public HttpRequest(String url,
                       String method,
                       String bodyType,
                       @Nullable String bodyAsString,
                       @Nullable byte[] bodyAsByteArray,
                       @Nullable List<byte[]> bodyAsMultipart) {
        this.url = url;
        this.method = method;
        this.bodyType = BodyType.valueOf(bodyType);
        this.bodyAsString = bodyAsString;
        this.bodyAsByteArray = bodyAsByteArray != null ?  Base64.getEncoder().encodeToString(bodyAsByteArray) : this.bodyAsByteArray;
        this.bodyAsMultipart = bodyAsMultipart != null ? convertMultipart(bodyAsMultipart) : this.bodyAsMultipart;

        if (BodyType.STRING==this.bodyType){
               if(!bodyAsString.isEmpty() &&this.bodyAsByteArray.isEmpty()&& this.bodyAsMultipart.isEmpty()){
                   LOGGER.debug("bodyType 'STRING' is accurate against bodyAsString,bodyAsByteArray and bodyAsMultipart fields");
               }else{
                   LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
                   throw new IllegalArgumentException("when bodyType is set to 'STRING', the 'bodyAsString' field must be non null ; 'bodyAsByteArray' and 'bodyAsMultipart' fields must be null ");
               }
        } else if(BodyType.BYTE_ARRAY.equals(this.bodyType)){
            if(bodyAsString.isEmpty() && !this.bodyAsByteArray.isEmpty() && this.bodyAsMultipart.isEmpty()){
                LOGGER.debug("bodyType 'BYTE_ARRAY' is accurate against bodyAsString,bodyAsByteArray and bodyAsMultipart fields");
            }else{
                LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
                throw new IllegalArgumentException("when bodyType is set to 'BYTE_ARRAY', the 'bodyAsString' and 'bodyAsMultipart' fields must be null ; 'bodyAsByteArray'  must be non-null");
            }
        }else if(BodyType.MULTIPART.equals(this.bodyType)){
            if(bodyAsString.isEmpty() && this.bodyAsByteArray.isEmpty() && this.bodyAsMultipart.size()>0){
                LOGGER.debug("bodyType 'MULTIPART' is accurate against bodyAsString,bodyAsByteArray and bodyAsMultipart fields");
            }else{
                LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
                throw new IllegalArgumentException("when 'bodyType' is set to 'MULTIPART', the 'bodyAsString' and 'bodyAsByteArray' fields must be null ; 'bodyAsMultipart' must be non-null");
            }
        }else{
            LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
            throw new IllegalArgumentException("'bodyType' is not set to either 'STRING','BYTE_ARRAY', or 'MULTIPART'");
        }
    }

    private List<String> convertMultipart(List<byte[]> bodyAsMultipart) {
        List<String> results = Lists.newArrayList();
        for (byte[] bytes : bodyAsMultipart) {
            results.add(Base64.getEncoder().encodeToString(bytes));
        }
        return results;
    }


    /**
     * only for json deserialization
     */
    protected HttpRequest() {
    }

    public void setBodyType(BodyType bodyType) {
        this.bodyType = bodyType;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Long getTimeoutInMs() {
        return timeoutInMs;
    }

    public void setTimeoutInMs(Long timeoutInMs) {
        this.timeoutInMs = timeoutInMs;
    }

    public Long getRetries() {
        return retries;
    }

    public void setRetries(Long retries) {
        this.retries = retries;
    }

    public Long getRetryDelayInMs() {
        return retryDelayInMs;
    }

    public void setRetryDelayInMs(Long retryDelayInMs) {
        this.retryDelayInMs = retryDelayInMs;
    }

    public Long getRetryMaxDelayInMs() {
        return retryMaxDelayInMs;
    }

    public void setRetryMaxDelayInMs(Long retryMaxDelayInMs) {
        this.retryMaxDelayInMs = retryMaxDelayInMs;
    }

    public Double getRetryDelayFactor() {
        return retryDelayFactor;
    }

    public void setRetryDelayFactor(Double retryDelayFactor) {
        this.retryDelayFactor = retryDelayFactor;
    }

    public Long getRetryJitter() {
        return retryJitter;
    }

    public void setRetryJitter(Long retryJitter) {
        this.retryJitter = retryJitter;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public String getUrl() {
        return url;
    }

    public String getMethod() {
        return method;
    }

    public String getBodyAsString() {
        return bodyAsString;
    }

    public byte[] getBodyAsByteArray() {
        return this.bodyAsByteArray!=null?Base64.getDecoder().decode(bodyAsByteArray):new byte[0];
    }

    public List<byte[]> getBodyAsMultipart() {
        List<byte[]> multipart = Lists.newArrayList();
        for (String encodedPart : this.bodyAsMultipart) {
            multipart.add(Base64.getDecoder().decode(encodedPart));
        }
        return multipart;
    }

    public BodyType getBodyType() {
        return bodyType;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRequest that = (HttpRequest) o;
        return Objects.equals(requestId, that.requestId) && Objects.equals(correlationId, that.correlationId) && Objects.equals(timeoutInMs, that.timeoutInMs) && Objects.equals(retries, that.retries) && Objects.equals(retryDelayInMs, that.retryDelayInMs) && Objects.equals(retryMaxDelayInMs, that.retryMaxDelayInMs) && Objects.equals(retryDelayFactor, that.retryDelayFactor) && Objects.equals(retryJitter, that.retryJitter) && url.equals(that.url) && Objects.equals(headers, that.headers) && method.equals(that.method) && Objects.equals(bodyAsString, that.bodyAsString) && Objects.equals(bodyAsByteArray, that.bodyAsByteArray) && Objects.equals(bodyAsMultipart, that.bodyAsMultipart) && bodyType == that.bodyType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, correlationId, timeoutInMs, retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitter, url, headers, method, bodyAsString, bodyAsByteArray, bodyAsMultipart, bodyType);
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "requestId='" + requestId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timeoutInMs=" + timeoutInMs +
                ", retries=" + retries +
                ", retryDelayInMs=" + retryDelayInMs +
                ", retryMaxDelayInMs=" + retryMaxDelayInMs +
                ", retryDelayFactor=" + retryDelayFactor +
                ", retryJitter=" + retryJitter +
                ", url='" + url + '\'' +
                ", headers=" + headers +
                ", method='" + method + '\'' +
                ", bodyAsString='" + bodyAsString + '\'' +
                ", bodyAsByteArray='" + bodyAsByteArray + '\'' +
                ", bodyAsMultipart=" + bodyAsMultipart +
                ", bodyType=" + bodyType +
                '}';
    }

    public Struct toStruct() {
        Struct struct =
                new Struct(SCHEMA)
                        .put(REQUEST_ID, requestId)
                        .put(CORRELATION_ID, correlationId)
                        .put(TIMEOUT_IN_MS, timeoutInMs)
                        .put(RETRIES, retries)
                        .put(RETRY_DELAY_IN_MS, retryDelayInMs)
                        .put(RETRY_MAX_DELAY_IN_MS, retryMaxDelayInMs)
                        .put(RETRY_DELAY_FACTOR, retryDelayFactor)
                        .put(RETRY_JITTER, retryJitter)
                        .put(URL, url)
                        .put(HEADERS, headers)
                        .put(METHOD, method)
                        .put(BODY_TYPE, bodyType.name())
                        .put(BODY_AS_STRING, bodyAsString)
                        .put(BODY_AS_BYTE_ARRAY, bodyAsByteArray)
                        .put(BODY_AS_MULTIPART, bodyAsMultipart);
        return struct;
    }


    public static final class Builder {

        private Struct struct;

        private Builder() {
        }

        public static Builder anHttpRequest() {
            return new Builder();
        }

        public Builder withStruct(Struct struct) {
            this.struct = struct;
            return this;
        }


        public HttpRequest build() {
            //request
            String url = struct.getString(URL);
            Map<String, List<String>> headers = struct.getMap(HEADERS);

            String method = struct.getString(METHOD);
            String bodyType = struct.getString(BODY_TYPE);
            String stringBody = struct.getString(BODY_AS_STRING);
            byte[] byteArrayBody = Base64.getDecoder().decode(struct.getString(BODY_AS_BYTE_ARRAY));
            List<byte[]> multipartBody = struct.getArray(BODY_AS_MULTIPART);

            HttpRequest httpRequest = new HttpRequest(
                    url,
                    method,
                    bodyType,
                    stringBody,
                    byteArrayBody,
                    multipartBody
            );

            httpRequest.setHeaders(headers);

            //metadata
            String requestId = struct.getString(REQUEST_ID);
            httpRequest.setRequestId(requestId);

            String correlationId = struct.getString(CORRELATION_ID);
            httpRequest.setCorrelationId(correlationId);

            //connection
            Long timeoutInMs = struct.getInt64(TIMEOUT_IN_MS);
            httpRequest.setTimeoutInMs(timeoutInMs);

            //retry policy
            Long retries = struct.getInt64(RETRIES);
            if (retries!=null && retries.longValue() >= 0) {
                httpRequest.setRetries(retries);
            }
            Long retryDelayInMs = struct.getInt64(RETRY_DELAY_IN_MS);
            if (retryDelayInMs!=null && retryDelayInMs >= 0) {
                httpRequest.setRetryDelayInMs(retryDelayInMs);
            }
            Long retryMaxDelayInMs = struct.getInt64(RETRY_MAX_DELAY_IN_MS);
            if (retryMaxDelayInMs!=null && retryMaxDelayInMs >= 0) {
                httpRequest.setRetryMaxDelayInMs(retryMaxDelayInMs);
            }
            Double retryDelayFactor = struct.getFloat64(RETRY_DELAY_FACTOR);
            if (retryDelayFactor!=null && retryDelayFactor >= 0) {
                httpRequest.setRetryDelayFactor(retryDelayFactor);
            }
            Long retryJitter = struct.getInt64(RETRY_JITTER);
            if (retryJitter!=null && retryJitter >= 0) {
                httpRequest.setRetryJitter(retryJitter);
            }
            return httpRequest;
        }
    }

    private enum BodyType {
        STRING,
        BYTE_ARRAY,
        MULTIPART

    }
}
