package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;

import static com.fasterxml.jackson.annotation.JsonInclude.*;
import static io.github.clescot.kafka.connect.http.core.Part.*;

@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpRequest.SCHEMA_AS_STRING,
        refs = {@io.confluent.kafka.schemaregistry.annotations.SchemaReference(name=Part.SCHEMA_ID, subject="httpPart",version = Part.VERSION)})
@JsonInclude(Include.NON_EMPTY)
public class HttpRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRequest.class);
    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";
    public static final String BODY_AS_FORM = "bodyAsForm";
    public static final String PARTS = "parts";
    public static final String MULTIPART_CONTENT_TYPE = "multipartContentType";
    public static final String MULTIPART_BOUNDARY = "multipartBoundary";

    public static final int VERSION = 2;
    public static final String CONTENT_TYPE = "Content-Type";


    //request
    @JsonProperty(required = true)
    private String url;
    @JsonProperty
    private Map<String, List<String>> headers = Maps.newHashMap();
    @JsonProperty(defaultValue = "GET")
    private HttpRequest.Method method;

    //regular body
    @JsonProperty
    private Map<String,String> bodyAsForm = Maps.newHashMap();
    @JsonProperty
    private String bodyAsString = null;
    @JsonProperty
    private String multipartBoundary=null;
    @JsonProperty
    private String bodyAsByteArray = null;

    @JsonProperty
    private List<Part> parts = Lists.newArrayList();

    @JsonProperty(defaultValue = "STRING")
    private BodyType bodyType;
    @JsonProperty(defaultValue = "multipart/form-data")
    private String multipartContentType;
    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(Part.class.getName())
            .version(VERSION)
            .field(URL,Schema.STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).build())
            .field(METHOD,Schema.STRING_SCHEMA)
            .field(BODY_TYPE,Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PARTS,SchemaBuilder.array(Part.SCHEMA).optional().schema())
            .field(MULTIPART_CONTENT_TYPE,Schema.OPTIONAL_STRING_SCHEMA)
            .field(MULTIPART_BOUNDARY,Schema.OPTIONAL_STRING_SCHEMA)
            .schema();

    public static final String SCHEMA_ID = HttpExchange.BASE_SCHEMA_ID+ VERSION + "/"+"http-request.json";
    public static final String SCHEMA_AS_STRING = "{\n" +
            "  \"$id\": \""+SCHEMA_ID+"\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema#\",\n" +
            "  \"title\": \"Http Request\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"additionalProperties\": false,\n" +
            "  \"properties\": {\n" +
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
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"bodyAsForm\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"connect.type\": \"map\",\n" +
            "      \"additionalProperties\": {\n" +
            "        \"type\": \"string\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"bodyAsByteArray\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"bodyAsMultipart\": {\n" +
            "      \"type\": \"array\",\n" +
            "      \"items\": {\n" +
            "        \"type\": \"string\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"bodyType\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"enum\": [\n" +
            "        \"STRING\",\n" +
            "        \"FORM\",\n" +
            "        \"BYTE_ARRAY\",\n" +
            "        \"MULTIPART\"\n" +
            "      ]\n" +
            "    },\n" +
            "    \"parts\": {\n" +
            "      \"type\": \"array\",\n" +
            "      \"connect.type\": \"map\",\n " +
            "      \"items\": {\n" +
            "        \"$ref\": \""+Part.SCHEMA_ID+"\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"url\",\n" +
            "    \"method\",\n" +
            "    \"bodyType\"\n" +
            "  ]\n" +
            "}";

    /**
     * only for json deserialization
     */
    protected HttpRequest() {
    }
    public HttpRequest(String url){
        this(url,HttpRequest.Method.GET,BodyType.STRING,null,null);
    }
    public HttpRequest(String url,HttpRequest.Method method){
        this(url,method,BodyType.STRING,null,null);
    }
    public HttpRequest(String url,
                       HttpRequest.Method method,
                       BodyType bodyType,
                       String multipartContentType,
                       String multipartBoundary) {
        Preconditions.checkNotNull(url, "url is required");
        Preconditions.checkNotNull(bodyType, "bodyType is required");
        this.url = url;
        Preconditions.checkNotNull(method, "'method' is required");
        this.method = method;
        this.multipartContentType = multipartContentType;
        this.multipartBoundary = multipartBoundary;
    }

    public HttpRequest(HttpRequest original) {
        this(original.getUrl(), original.getMethod(), original.getBodyType(),original.getMultipartContentType(), original.getMultipartBoundary());
        this.setHeaders(Maps.newHashMap(original.getHeaders()));
        this.setParts(Lists.newArrayList(original.getParts()));
    }

    public HttpRequest(Struct struct) {
        this.url = struct.getString(URL);
        Preconditions.checkNotNull(url, "'url' is required");

        Map<String, List<String>> headers = struct.getMap(HEADERS);
        if (headers != null && !headers.isEmpty()) {
            this.headers = headers;
        }

        this.method = HttpRequest.Method.valueOf(struct.getString(METHOD).toUpperCase());
        Preconditions.checkNotNull(method, "'method' is required");

        this.bodyAsByteArray = struct.getString(BODY_AS_BYTE_ARRAY);
        this.bodyAsString = struct.getString(BODY_AS_STRING);
        this.bodyAsForm = struct.getMap(BODY_AS_FORM);

        this.multipartContentType = struct.getString(MULTIPART_CONTENT_TYPE);
        this.multipartBoundary = struct.getString(MULTIPART_BOUNDARY);
        this.parts = struct.getArray(PARTS);
        if (parts != null) {
            //this is a multipart request
            for (Part part : parts) {
                if (!headersFromPartAreValid(part)) {
                    LOGGER.warn("this is a multipart request. headers from part are not valid : there is at least one header that is not 'Content-Disposition', 'Content-Type' or 'Content-Transfer-Encoding'. clearing headers from this part");
                    part.getHeaders().clear();
                }
            }
        }

    }

    private boolean headersFromPartAreValid(Part part) {
        Map<String, List<String>> headersFromPart = part.getHeaders();
        if (headersFromPart != null && !headersFromPart.isEmpty()) {
            return headersFromPart.keySet().stream()
                    .filter(key -> !key.equalsIgnoreCase("Content-Disposition"))
                    .filter(key -> !key.equalsIgnoreCase("Content-Type"))
                    .filter(key -> !key.equalsIgnoreCase("Content-Transfer-Encoding"))
                    .findAny().isEmpty();

        }
        return true;
    }

    public BodyType getBodyType(){
        return bodyType;
    }

    public String getMultipartContentType() {
        return multipartContentType;
    }

    public void setMultipartContentType(String multipartContentType) {
        this.multipartContentType = multipartContentType;
    }

    public List<Part> getParts() {
        return parts;
    }

    public void setParts(List<Part> parts) {
        this.parts = parts;
    }

    public void addPart(Part part) {
        parts.add(part);
    }



    public Map<String, List<String>> getHeaders() {
       return headers;
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

    public String getMultipartBoundary() {
        return multipartBoundary;
    }

    public void setMultipartBoundary(String multipartBoundary) {
        this.multipartBoundary = multipartBoundary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRequest that = (HttpRequest) o;
        return url.equals(that.url)
                && Objects.equals(headers, that.headers)
                && method.equals(that.method)
                && Objects.equals(parts, that.parts)
                && Objects.equals(multipartContentType, that.multipartContentType)
                && Objects.equals(multipartBoundary, that.multipartBoundary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, headers, method, parts, multipartContentType, multipartBoundary);
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "bodyAsByteArray='" + bodyAsByteArray + '\'' +
                ", url='" + url + '\'' +
                ", headers=" + headers +
                ", method=" + method +
                ", bodyAsForm=" + bodyAsForm +
                ", bodyAsString='" + bodyAsString + '\'' +
                ", multipartBoundary='" + multipartBoundary + '\'' +
                ", parts=" + parts +
                ", bodyType=" + bodyType +
                ", multipartContentType='" + multipartContentType + '\'' +
                '}';
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(URL, this.getUrl())
                .put(HEADERS, this.getHeaders())
                .put(METHOD, this.getMethod().name())
                .put(PARTS, this.getParts().stream().map(Part::toStruct).collect(Collectors.toList()))
                .put(MULTIPART_CONTENT_TYPE, this.getMultipartContentType())
                .put(MULTIPART_BOUNDARY, this.getMultipartBoundary())
                ;
    }

    public void setBodyAsString(String bodyAsString) {
        this.bodyAsString = bodyAsString;
    }

    public void setBodyAsByteArray(byte[] content) {
        if(content!=null && content.length>0) {
            bodyAsByteArray = Base64.getEncoder().encodeToString(content);
        }
    }

    @JsonIgnore
    public byte[] getBodyAsByteArray() {
        if(bodyAsByteArray!=null && !bodyAsByteArray.isEmpty()) {
            return Base64.getDecoder().decode(bodyAsByteArray);
        }
        return null;
    }


    public void setBodyAsForm(Map<String, String> form) {
        this.bodyAsForm = form;
    }

    @JsonIgnore
    public String getBodyAsString() {
        return this.bodyAsString;
    }


    @JsonIgnore
    public Map<String, String> getBodyAsForm() {
        return bodyAsForm;
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
