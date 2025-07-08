package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HttpResponse implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;
    public static final Integer VERSION = 2;
    public static final String CONTENT_TYPE = "Content-Type";

    public static final String STATUS_CODE = "statusCode";
    public static final String STATUS_MESSAGE = "statusMessage";
    public static final String PROTOCOL = "protocol";
    public static final String HEADERS = "headers";
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";
    public static final String BODY_AS_FORM = "bodyAsForm";
    public static final String PARTS = "parts";

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE, Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE, Schema.STRING_SCHEMA)
            .field(PROTOCOL, Schema.OPTIONAL_STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY_TYPE, Schema.STRING_SCHEMA)
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PARTS, SchemaBuilder.array(HttpPart.SCHEMA).optional().schema())
            .schema();

    @JsonProperty(required = true)
    private Integer statusCode;
    @JsonProperty(required = true)
    private String statusMessage;
    @JsonProperty
    private Map<String, String> bodyAsForm = Maps.newHashMap();
    @JsonProperty
    private String bodyAsString = "";
    @JsonProperty
    //byte array is base64 encoded as a String, as JSON is a text format not binary
    private String bodyAsByteArray = "";
    @JsonProperty(defaultValue = "STRING")
    private HttpResponse.BodyType bodyType = HttpResponse.BodyType.STRING;
    @JsonProperty
    private String protocol = "";
    @JsonProperty
    private Map<String, HttpPart> parts = Maps.newHashMap();
    @JsonProperty
    private Map<String, List<String>> headers = Maps.newHashMap();

    /**
     * only for json deserialization
     */
    protected HttpResponse() {
    }

    public HttpResponse(Integer statusCode, String statusMessage) {
        Preconditions.checkArgument(statusCode > 0, "status code must be a positive integer");
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getBodyAsString() {
        return bodyAsString;
    }


    public Map<String, HttpPart> getParts() {
        return parts;
    }

    public void setParts(Map<String, HttpPart> parts) {
        this.parts = parts;
        if(parts!=null && !parts.isEmpty()) {
            bodyType = HttpResponse.BodyType.MULTIPART;
        }
    }

    @JsonIgnore
    public Map<String, String> getBodyAsForm() {
        return bodyAsForm;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    public void setBodyAsString(String bodyAsString) {
        this.bodyAsString = bodyAsString;
    }

    public void setBodyAsForm(Map<String, String> form) {
        this.bodyAsForm = form;
        bodyType = HttpResponse.BodyType.FORM;
        if (form != null && !form.isEmpty() && headers != null && doesNotContainHeader(CONTENT_TYPE)) {
            headers.put(CONTENT_TYPE, Lists.newArrayList("application/x-www-form-urlencoded"));
        }
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

    public void setBodyAsByteArray(byte[] content) {
        if (content != null && content.length > 0) {
            bodyAsByteArray = Base64.getEncoder().encodeToString(content);
            bodyType = HttpResponse.BodyType.BYTE_ARRAY;

            //if no Content-Type is set, we set the default application/octet-stream
            if (headers != null && doesNotContainHeader(CONTENT_TYPE)) {
                headers.put(CONTENT_TYPE, Lists.newArrayList("application/octet-stream"));
            }
        }
    }

    @JsonIgnore
    public byte[] getBodyAsByteArray() {
        if (bodyAsByteArray != null && !bodyAsByteArray.isEmpty()) {
            return Base64.getDecoder().decode(bodyAsByteArray);
        }
        return null;
    }

    public BodyType getBodyType() {
        return bodyType;
    }

    private boolean doesNotContainHeader(String key) {
        return headers.keySet().stream().filter(k -> k.equalsIgnoreCase(key)).findAny().isEmpty();
    }

    @JsonIgnore
    public long getHeadersLength() {
        return headers.entrySet().stream()
                .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
                .mapToLong(entry -> entry.getKey().length() + entry.getValue().stream().mapToLong(String::length).sum())
                .sum();
    }

    @JsonIgnore
    public long getLength() {
        return getHeadersLength() + getBodyContentLength();
    }


    @JsonIgnore
    public Long getBodyContentLength() {
        if (getHeaders().containsKey("Content-Length")) {
            List<String> values = getHeaders().get("Content-Length");
            if (values != null && !values.isEmpty()) {
                try {
                    return Long.parseLong(values.get(0));
                } catch (NumberFormatException e) {
                    // If parsing fails, we will calculate the content length based on the body type
                    return getBodyContentLengthFromBodyType();
                }
            }
        }
        return getBodyContentLengthFromBodyType();
    }
    private long getBodyContentLengthFromBodyType() {
        if (BodyType.STRING == bodyType) {
            return bodyAsString.getBytes().length;
        } else if (BodyType.BYTE_ARRAY == bodyType) {
            return getBodyAsByteArray() != null ? getBodyAsByteArray().length : 0;
        } else if (BodyType.FORM == bodyType) {
            return bodyAsForm != null ?
                    bodyAsForm
                            .entrySet()
                            .stream()
                            .filter(pair->pair.getValue()!=null)
                            .map(pair->pair.getKey().length()+pair.getValue().length())
                            .reduce(Integer::sum).orElse(0): 0;
        } else if (BodyType.MULTIPART == bodyType) {
            return parts.values().stream().mapToLong(HttpPart::getBodyContentLength).sum();
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpResponse that = (HttpResponse) o;
        return
            statusCode.equals(that.statusCode) &&
            statusMessage.equals(that.statusMessage) &&
            protocol.equals(that.protocol) &&
            bodyType == that.bodyType &&
            Objects.equals(headers, that.headers) &&
            bodyAsString.equals(that.bodyAsString) &&
            Objects.equals(bodyAsByteArray, that.bodyAsByteArray) &&
            Objects.equals(bodyAsForm, that.bodyAsForm) &&
            Objects.deepEquals(parts, that.parts)
            ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusCode, statusMessage, protocol, bodyType, bodyAsString, headers, bodyAsByteArray);
    }

    @Override
    public String toString() {
        return "HttpResponse{" +
                "statusCode=" + statusCode +
                ", statusMessage='" + statusMessage + '\'' +
                ", protocol='" + protocol + '\'' +
                ", headers=" + headers +
                ", bodyAsByteArray='" + bodyAsByteArray + '\'' +
                ", bodyAsForm='" + bodyAsByteArray + '\'' +
                ", bodyAsString='" + bodyAsString + '\'' +
                '}';
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(STATUS_CODE, this.getStatusCode().longValue())
                .put(STATUS_MESSAGE, this.getStatusMessage())
                .put(PROTOCOL, this.getProtocol())
                .put(HEADERS, this.getHeaders())
                .put(BODY_TYPE, this.getBodyType().toString())
                .put(BODY_AS_BYTE_ARRAY, this.bodyAsByteArray)
                .put(BODY_AS_STRING, this.getBodyAsString())
                ;
    }

    @Override
    public Object clone() {
        try {
            HttpResponse clone = (HttpResponse) super.clone();
            clone.setStatusCode(this.statusCode);
            clone.setStatusMessage(this.statusMessage);
            clone.setProtocol(this.protocol);
            clone.setBodyAsString(this.bodyAsString);
            clone.setBodyAsForm(Maps.newHashMap(this.bodyAsForm));
            clone.setBodyAsByteArray(this.getBodyAsByteArray());
            clone.setHeaders(Maps.newHashMap(this.headers));
            clone.parts = Maps.newHashMap(this.parts);
            clone.bodyType = this.bodyType;
            clone.headers = Maps.newHashMap(this.headers);
            if (this.parts != null) {
                clone.parts = Maps.newHashMap(this.parts);
            } else {
                clone.parts = Maps.newHashMap();
            }
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public enum BodyType {
        STRING,
        BYTE_ARRAY,
        FORM,
        MULTIPART;

        @Override
        public String toString() {
            return name();
        }
    }
}
