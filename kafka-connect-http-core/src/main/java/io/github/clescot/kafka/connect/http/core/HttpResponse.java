package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.sstoehr.harreader.model.HarHeader;
import de.sstoehr.harreader.model.HarResponse;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

import static io.github.clescot.kafka.connect.http.core.MediaType.APPLICATION_OCTET_STREAM;
import static io.github.clescot.kafka.connect.http.core.MediaType.APPLICATION_X_WWW_FORM_URLENCODED;

public class HttpResponse implements Response, Cloneable, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public static final Integer VERSION = 2;
    public static final String RETRY_AFTER = "Retry-After";
    public static final String X_RETRY_AFTER = "X-Retry-After";
    public static final String STATUS_CODE_FIELD = "statusCode";
    public static final String STATUS_MESSAGE_FIELD = "statusMessage";
    public static final String PROTOCOL_FIELD = "protocol";
    public static final String HEADERS_FIELD = "headers";
    public static final String BODY_TYPE_FIELD = "bodyType";
    public static final String BODY_AS_STRING_FIELD = "bodyAsString";
    public static final String BODY_AS_BYTE_ARRAY_FIELD = "bodyAsByteArray";
    public static final String BODY_AS_FORM_FIELD = "bodyAsForm";
    public static final String PARTS_FIELD = "parts";
    public static final String ATTRIBUTES_FIELD = "attributes";
    private Integer statusMessageLimit = Integer.MAX_VALUE;
    private Integer headersLimit = Integer.MAX_VALUE;
    private Integer bodyLimit = Integer.MAX_VALUE;

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE_FIELD, Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE_FIELD, Schema.STRING_SCHEMA)
            .field(PROTOCOL_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(HEADERS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY_TYPE_FIELD, Schema.STRING_SCHEMA)
            .field(BODY_AS_BYTE_ARRAY_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_STRING_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PARTS_FIELD, SchemaBuilder.array(HttpPart.SCHEMA).optional().schema())
            .field(ATTRIBUTES_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
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
    private BodyType bodyType = BodyType.STRING;
    @JsonProperty
    private String protocol = "";
    @JsonProperty
    private Map<String, HttpPart> parts = Maps.newHashMap();
    @JsonProperty
    private Map<String, List<String>> headers = Maps.newHashMap();
    @JsonProperty
    private Map<String, Object> attributes = Maps.newHashMap();

    /**
     * only for json deserialization
     */
    protected HttpResponse() {
    }

    public HttpResponse(Integer statusCode, String statusMessage) {
        this(statusCode, statusMessage, null, null, null);
    }

    public HttpResponse(Integer statusCode, String statusMessage, Integer statusMessageLimit, Integer headersLimit, Integer bodyLimit) {
        this.statusCode = statusCode;
        if (headersLimit != null) {
            this.headersLimit = Math.max(0, headersLimit);
        }

        if (statusMessageLimit != null) {
            this.statusMessageLimit = Math.max(0, statusMessageLimit);
        }
        if (statusMessage != null && statusMessageLimit != null) {
            this.statusMessage = statusMessage.substring(0, Math.min(statusMessage.length(), statusMessageLimit));
        } else {
            this.statusMessage = statusMessage;
        }

        if (bodyLimit != null) {
            this.bodyLimit = Math.max(0, bodyLimit);
        }
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    @Override
    @JsonIgnore
    public boolean isSuccess() {
        return statusCode != null && statusCode >= 200 && statusCode < 400;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getBodyAsString() {
        return bodyAsString;
    }

    @JsonIgnore
    public List<String> getContentType() {
        return headers.get(MediaType.KEY);
    }


    public void setContentType(String contentType) {
        if (contentType != null && !contentType.isEmpty()) {
            headers.put(MediaType.KEY, Lists.newArrayList(contentType));
            bodyType = BodyType.getBodyType(contentType);
        } else {
            headers.remove(MediaType.KEY);
            bodyType = BodyType.STRING;
        }
    }

    /**
     *
     * @return the value of the Retry-After header, either Retry-After or X-Retry-After.
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After">Retry-After</a>
     */
    public String getRetryAfterValue() {
        return getHeaders().get(RETRY_AFTER) != null ? getHeaders().get(RETRY_AFTER).get(0) : (getHeaders().get(X_RETRY_AFTER) != null ? getHeaders().get(X_RETRY_AFTER).get(0) : null);
    }



    public Map<String, HttpPart> getParts() {
        return parts;
    }

    public void setParts(Map<String, HttpPart> parts) {
        this.parts = parts;
        if (parts != null && !parts.isEmpty()) {
            bodyType = BodyType.MULTIPART;
        }
    }

    @JsonIgnore
    public Map<String, String> getBodyAsForm() {
        return bodyAsForm;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        Map<String, List<String>> headersWithLimit = Maps.newHashMap();
        if (headers != null) {
            int headersSize = 0;
            for (Map.Entry<String, List<String>> next : headers.entrySet()) {
                int keyLength = next.getKey().length();
                if (headersSize + keyLength < headersLimit) {
                    headersSize += keyLength;
                    Iterator<String> valuesIterator = next.getValue().iterator();
                    List<String> valuesWithLimit = Lists.newArrayList();
                    while (valuesIterator.hasNext()) {
                        String myValue = valuesIterator.next();
                        if (myValue != null && headersSize + myValue.length() < headersLimit) {
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

    public void setBodyAsString(String bodyAsString) {
        if (bodyAsString != null) {
            this.bodyAsString = bodyAsString.substring(0, Math.min(bodyAsString.length(), bodyLimit));
            this.bodyType = BodyType.STRING;
        }
    }

    public void setBodyAsByteArray(byte[] content) {
        if (content == null) {
            this.bodyAsByteArray = "";
            return;
        }
        Preconditions.checkArgument(bodyLimit >= content.length, "bodyAsByteArray length exceeds bodyLimit");

        if (content != null && content.length > 0) {
            this.bodyAsByteArray = Base64.getEncoder().encodeToString(content);
            this.bodyType = BodyType.BYTE_ARRAY;

            //if no Content-Type is set, we set the default application/octet-stream
            if (this.headers != null && doesNotContainHeader(MediaType.KEY)) {
                this.headers.put(MediaType.KEY, Lists.newArrayList(APPLICATION_OCTET_STREAM));
            }
        }

    }

    public void setBodyAsForm(Map<String, String> form) {
        this.bodyAsForm = form;
        bodyType = BodyType.FORM;
        if (form != null && !form.isEmpty() && headers != null && doesNotContainHeader(MediaType.KEY)) {
            headers.put(MediaType.KEY, Lists.newArrayList(APPLICATION_X_WWW_FORM_URLENCODED));
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


    @JsonIgnore
    public byte[] getBodyAsByteArray() {
        if (bodyAsByteArray != null && !bodyAsByteArray.isEmpty()) {
            return Base64.getDecoder().decode(bodyAsByteArray);
        }
        return new byte[0];
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
                            .filter(pair -> pair.getValue() != null)
                            .map(pair -> pair.getKey().length() + pair.getValue().length())
                            .reduce(Integer::sum).orElse(0) : 0;
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
                        attributes.equals(that.attributes) &&
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
        return Objects.hash(statusCode, attributes, statusMessage, protocol, bodyType, bodyAsString, headers, bodyAsByteArray);
    }

    @Override
    public String toString() {
        return "HttpResponse{" +
                "attributes=" + attributes +
                ", statusCode=" + statusCode + '\'' +
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
                .put(STATUS_CODE_FIELD, this.getStatusCode().longValue())
                .put(STATUS_MESSAGE_FIELD, this.getStatusMessage())
                .put(PROTOCOL_FIELD, this.getProtocol())
                .put(HEADERS_FIELD, this.getHeaders())
                .put(BODY_TYPE_FIELD, this.getBodyType().toString())
                .put(BODY_AS_BYTE_ARRAY_FIELD, this.bodyAsByteArray)
                .put(BODY_AS_STRING_FIELD, this.getBodyAsString())
                .put(ATTRIBUTES_FIELD, this.getAttributes())
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

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public static HttpResponse fromHarResponse(HarResponse response){
        HttpResponse httpResponse = new HttpResponse(response.status(), response.statusText());
        httpResponse.setProtocol(response.httpVersion());
        if(response.headers()!=null){
            Map<String, List<String>> headers = Maps.newHashMap();
            for(HarHeader harHeader:response.headers()){
                if(!headers.containsKey(harHeader.name())){
                    headers.put(harHeader.name(), Lists.newArrayList(harHeader.value()));
                }else{
                    headers.get(harHeader.name()).add(harHeader.value());
                }
            }
            httpResponse.setHeaders(headers);
        }
        if(response.content()!=null){
            if(response.content().text()!=null){
                if("base64".equalsIgnoreCase(response.content().encoding())){
                    httpResponse.setBodyAsByteArray(Base64.getDecoder().decode(response.content().text()));
                }else{
                    httpResponse.setBodyAsString(response.content().text());
                }
            }
        }
        return httpResponse;

    }

    public HarResponse toHarResponse() {
        HarResponse.HarResponseBuilder harResponseBuilder = HarResponse.builder();
        harResponseBuilder.status(this.getStatusCode());
        harResponseBuilder.statusText(this.getStatusMessage());
        harResponseBuilder.httpVersion(this.getProtocol());
        if (this.getHeaders() != null) {
            List<HarHeader> harHeaders = this.getHeaders().entrySet().stream().map(
                    entry -> {
                        HarHeader.HarHeaderBuilder harHeader = HarHeader.builder();
                        harHeader.name(entry.getKey());
                        harHeader.value(Joiner.on("; ").join(entry.getValue()));
                        return harHeader.build();
                    }).toList();
            harResponseBuilder.headers(harHeaders);
        }
        harResponseBuilder.content(de.sstoehr.harreader.model.HarContent.builder()
                .size(this.getBodyContentLength())
                .mimeType(this.getContentType() != null && !this.getContentType().isEmpty() ? this.getContentType().get(0) : "")
                .text(this.getBodyAsString())
                .encoding(this.bodyType == BodyType.BYTE_ARRAY ? "base64" : null)
                .build());
        return harResponseBuilder.build();
    }
}
