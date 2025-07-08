package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class HttpRequest implements Cloneable, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRequest.class);
    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";
    public static final String BODY_AS_FORM = "bodyAsForm";
    public static final String PARTS = "parts";

    public static final int VERSION = 2;
    public static final String CONTENT_TYPE = "Content-Type";
    private static final Pattern BOUNDARY = Pattern.compile(".*; boundary=(.*)");

    //request
    @JsonProperty(required = true)
    private String url;
    @JsonProperty
    private Map<String, List<String>> headers = Maps.newHashMap();
    @JsonProperty(defaultValue = "GET")
    private HttpRequest.Method method;

    //regular body
    @JsonProperty
    private Map<String, String> bodyAsForm = Maps.newHashMap();
    @JsonProperty
    private String bodyAsString = "";
    @JsonProperty
    //byte array is base64 encoded as as String, as JSON is a text format not binary
    private String bodyAsByteArray = "";

    @JsonProperty
    private Map<String,HttpPart> parts = Maps.newHashMap();

    @JsonProperty(defaultValue = "STRING")
    private BodyType bodyType;
    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpPart.class.getName())
            .version(VERSION)
            .field(URL, Schema.STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).build())
            .field(METHOD, Schema.STRING_SCHEMA)
            .field(BODY_TYPE, Schema.STRING_SCHEMA)
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PARTS, SchemaBuilder.map(Schema.STRING_SCHEMA,HttpPart.SCHEMA).optional().schema())
            .schema();

    /**
     * only for json deserialization
     */
    protected HttpRequest() {
    }

    public HttpRequest(String url) {
        this(url, HttpRequest.Method.GET, Maps.newHashMap(), BodyType.STRING, null);
    }

    public HttpRequest(String url, HttpRequest.Method method) {
        this(url, method, Maps.newHashMap(), BodyType.STRING, null);
    }

    public HttpRequest(String url, HttpRequest.Method method, Map<String, List<String>> headers) {
        this(url, method, headers, BodyType.STRING, null);
    }

    public HttpRequest(String url, HttpRequest.Method method, Map<String, List<String>> headers, BodyType bodyType) {
        this(url, method, headers, bodyType, null);
    }

    public HttpRequest(String url,
                       HttpRequest.Method method,
                       Map<String, List<String>> headers,
                       BodyType bodyType,
                       Map<String,HttpPart> parts) {
        Preconditions.checkNotNull(url, "url is required");
        Preconditions.checkNotNull(bodyType, "bodyType is required");
        this.url = url;
        Preconditions.checkNotNull(method, "'method' is required");
        this.method = method;
        this.headers = MoreObjects.firstNonNull(headers, Maps.newHashMap());
        this.bodyType = bodyType;
        if (parts != null && !parts.isEmpty()) {
            if (StringUtils.isEmpty(getContentType())) {
                //default multipart Content-Type
                setContentType("multipart/form-data; boundary=" + UUID.randomUUID());
            }
            this.parts = parts;
        } else {
            this.parts = Maps.newHashMap();
        }


    }




    public HttpRequest(Struct requestAsstruct) {
        this.url = requestAsstruct.getString(URL);
        Preconditions.checkNotNull(url, "'url' is required");

        Map<String, List<String>> headers = requestAsstruct.getMap(HEADERS);
        if (headers != null && !headers.isEmpty()) {
            this.headers = headers;
        } else {
            this.headers = Maps.newHashMap();
        }

        this.method = HttpRequest.Method.valueOf(requestAsstruct.getString(METHOD).toUpperCase());
        Preconditions.checkNotNull(method, "'method' is required");

        this.bodyType = BodyType.valueOf(Optional.ofNullable(requestAsstruct.getString(BODY_TYPE)).orElse(BodyType.STRING.name()));

        this.bodyAsByteArray = requestAsstruct.getString(BODY_AS_BYTE_ARRAY);
        this.bodyAsString = requestAsstruct.getString(BODY_AS_STRING);
        this.bodyAsForm = requestAsstruct.getMap(BODY_AS_FORM);

        Map<String,Struct> structs = requestAsstruct.getMap(PARTS);
        if (structs != null) {
            //this is a multipart request
            for (Map.Entry<String,Struct> entry : structs.entrySet()) {
                HttpPart httpPart = new HttpPart(entry.getValue());
                parts.put(entry.getKey(),httpPart);
                if (!headersFromPartAreValid(httpPart)) {
                    LOGGER.warn("this is a multipart request. headers from part are not valid : there is at least one header that is not 'Content-Disposition', 'Content-Type' or 'Content-Transfer-Encoding'. clearing headers from this part");
                    httpPart.getHeaders().clear();
                }
            }
        }

    }

    private boolean headersFromPartAreValid(HttpPart httpPart) {
        Map<String, List<String>> headersFromPart = httpPart.getHeaders();
        if (headersFromPart != null && !headersFromPart.isEmpty()) {
            return headersFromPart.keySet().stream()
                    .filter(key -> !key.equalsIgnoreCase("Content-Disposition"))
                    .filter(key -> !key.equalsIgnoreCase(CONTENT_TYPE))
                    .filter(key -> !key.equalsIgnoreCase("Content-Transfer-Encoding"))
                    .findAny().isEmpty();

        }
        return true;
    }

    public BodyType getBodyType() {
        return bodyType;
    }

    public Map<String,HttpPart> getParts() {
        return parts;
    }

    public void setParts(Map<String,HttpPart> httpParts) {
        this.parts = httpParts;
        if (parts != null && !parts.isEmpty()) {
            this.bodyType = BodyType.MULTIPART;
            if(StringUtils.isEmpty(getContentType())){
                //default multipart Content-Type
                setContentType("multipart/form-data; boundary=" + UUID.randomUUID());
            }
        }
    }

    public void addPart(String name,HttpPart httpPart) {
        if(parts.isEmpty() && StringUtils.isEmpty(getContentType())){
            //default multipart Content-Type
            setContentType("multipart/form-data; boundary=" + UUID.randomUUID());
        }
        parts.put(name,httpPart);
    }

    @JsonIgnore
    public String getContentType() {
        if (headers != null
                && headers.containsKey(CONTENT_TYPE)
                && headers.get(CONTENT_TYPE) != null
                && !headers.get(CONTENT_TYPE).isEmpty()) {
            return headers.get(CONTENT_TYPE).get(0);
        }
        return null;
    }

    @JsonIgnore
    public String getBoundary() {
        String contentType = getContentType();
        String boundary = null;
        if (!StringUtils.isEmpty(contentType)) {
            Matcher matcher = BOUNDARY.matcher(contentType);
            if (matcher.matches()) {
                boundary = matcher.group(1);
            }
        }
        return boundary;
    }

    public void setContentType(String contentType) {
        headers.put(CONTENT_TYPE, Lists.newArrayList(contentType));
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
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

    public String getUrl() {
        return url;
    }

    public HttpRequest.Method getMethod() {
        return method;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRequest that = (HttpRequest) o;
        return url.equals(that.url)
                && Objects.equals(headers, that.headers)
                && method.equals(that.method)
                && bodyType == that.bodyType
                && Objects.equals(bodyAsByteArray, that.bodyAsByteArray)
                && Objects.equals(bodyAsForm, that.bodyAsForm)
                && Objects.equals(bodyAsString, that.bodyAsString)
                && Objects.deepEquals(parts, that.parts)
                ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, headers, method, parts, bodyAsByteArray, bodyAsForm, bodyAsString,bodyType);
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "  url='" + url + '\'' +
                ", headers=" + headers +
                ", method=" + method +
                ", bodyAsByteArray='" + bodyAsByteArray + '\'' +
                ", bodyAsForm=" + bodyAsForm +
                ", bodyAsString='" + bodyAsString + '\'' +
                ", parts=" + parts +
                ", bodyType=" + bodyType +
                '}';
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(URL, this.getUrl())
                .put(HEADERS, this.getHeaders())
                .put(METHOD, this.getMethod().name())
                .put(BODY_TYPE, this.getBodyType().name())
                .put(BODY_AS_BYTE_ARRAY, this.bodyAsByteArray)
                .put(BODY_AS_FORM, this.getBodyAsForm())
                .put(BODY_AS_STRING, this.getBodyAsString())
                .put(PARTS,
                        this.getParts().entrySet().stream()
                                .collect(
                                        Collectors.toMap(Map.Entry::getKey,
                                                entry->entry.getValue().toStruct())
                                )
                )
                ;
    }

    public void setBodyAsString(String bodyAsString) {
        this.bodyAsString = bodyAsString;
        this.bodyType = BodyType.STRING;
    }

    public void setBodyAsByteArray(byte[] content) {
        if (content != null && content.length > 0) {
            bodyAsByteArray = Base64.getEncoder().encodeToString(content);
            bodyType = BodyType.BYTE_ARRAY;

            //if no Content-Type is set, we set the default application/octet-stream
            if (headers != null && doesNotContainHeader(CONTENT_TYPE)) {
                headers.put(CONTENT_TYPE, Lists.newArrayList("application/octet-stream"));
            }
        }
    }

    private boolean doesNotContainHeader(String key) {
        return headers.keySet().stream().filter(k -> k.equalsIgnoreCase(key)).findAny().isEmpty();
    }

    @JsonIgnore
    public long getBodyContentLength() {
        if (BodyType.STRING == bodyType) {
            return bodyAsString != null ? bodyAsString.length() : 0;
        } else if (BodyType.BYTE_ARRAY == bodyType) {
            return bodyAsByteArray != null ? getBodyAsByteArray().length : 0;
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

    @JsonIgnore
    public byte[] getBodyAsByteArray() {
        if (bodyAsByteArray != null && !bodyAsByteArray.isEmpty()) {
            return Base64.getDecoder().decode(bodyAsByteArray);
        }
        return null;
    }


    public void setBodyAsForm(Map<String, String> form) {
        this.bodyAsForm = form;
        bodyType = BodyType.FORM;
        if (form!=null && !form.isEmpty() && headers != null && doesNotContainHeader(CONTENT_TYPE)) {
            headers.put(CONTENT_TYPE, Lists.newArrayList("application/x-www-form-urlencoded"));
        }
    }

    @JsonIgnore
    public String getBodyAsString() {
        return this.bodyAsString;
    }


    @JsonIgnore
    public Map<String, String> getBodyAsForm() {
        return bodyAsForm;
    }

    @Override
    public Object clone() {
        try {
            HttpRequest clone = (HttpRequest) super.clone();
            clone.setHeaders(Maps.newHashMap(this.getHeaders()));
            clone.setParts(Maps.newHashMap(this.getParts()));
            clone.setBodyAsByteArray(this.getBodyAsByteArray());
            clone.setBodyAsForm(new HashMap<>(this.getBodyAsForm()));
            clone.setBodyAsString(this.getBodyAsString());
            clone.bodyType = this.getBodyType();
            clone.method = this.getMethod();
            clone.url = this.getUrl();
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

    public enum Method {
        CONNECT,
        DELETE,
        GET,
        HEAD,
        PATCH,
        POST,
        PUT,
        OPTIONS,
        TRACE
    }

}
