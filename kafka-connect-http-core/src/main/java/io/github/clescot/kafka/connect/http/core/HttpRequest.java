package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpRequest.SCHEMA_AS_STRING,
        refs = {@io.confluent.kafka.schemaregistry.annotations.SchemaReference(name= HttpPart.SCHEMA_ID, subject="httpPart",version = HttpPart.VERSION)})
@JsonInclude(Include.NON_EMPTY)
public class HttpRequest implements Serializable {

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
    private Map<String,String> bodyAsForm = Maps.newHashMap();
    @JsonProperty
    private String bodyAsString = null;
    @JsonProperty
    //byte array is base64 encoded as as String, as JSON is a text format not binary
    private String bodyAsByteArray = null;

    @JsonProperty
    private List<HttpPart> parts = Lists.newArrayList();

    @JsonProperty(defaultValue = "STRING")
    private BodyType bodyType;
    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpPart.class.getName())
            .version(VERSION)
            .field(URL,Schema.STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).build())
            .field(METHOD,Schema.STRING_SCHEMA)
            .field(BODY_TYPE,Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PARTS,SchemaBuilder.array(HttpPart.SCHEMA).optional().schema())
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
            "        \"$ref\": \""+ HttpPart.SCHEMA_ID+"\"\n" +
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
        this(url,HttpRequest.Method.GET,Maps.newHashMap(),BodyType.STRING,null);
    }
    public HttpRequest(String url,HttpRequest.Method method){
        this(url,method,Maps.newHashMap(),BodyType.STRING,null);
    }
    public HttpRequest(String url,HttpRequest.Method method,Map<String, List<String>> headers){
        this(url,method,headers,BodyType.STRING,null);
    }
    public HttpRequest(String url,HttpRequest.Method method,Map<String, List<String>> headers,BodyType bodyType){
        this(url,method,headers,bodyType,null);
    }
    public HttpRequest(String url,
                       HttpRequest.Method method,
                       Map<String, List<String>> headers,
                       BodyType bodyType,
                       List<HttpPart> parts) {
        Preconditions.checkNotNull(url, "url is required");
        Preconditions.checkNotNull(bodyType, "bodyType is required");
        this.url = url;
        Preconditions.checkNotNull(method, "'method' is required");
        this.method = method;
        this.headers = MoreObjects.firstNonNull(headers,Maps.newHashMap());
        this.bodyType = bodyType;
        if(parts!=null && !parts.isEmpty()){
            if(getContentType()==null){
                //default multipart Content-Type
                setContentType("multipart/form-data; boundary="+UUID.randomUUID());
            }
            this.parts = parts;
        }else{
            this.parts = Lists.newArrayList();
        }


    }

    public HttpRequest(HttpRequest original) {
        this(
                original.getUrl(),
                original.getMethod(),
                original.getHeaders(),
                original.getBodyType(),
                original.getParts()
        );
        this.setHeaders(Maps.newHashMap(original.getHeaders()));
        this.setParts(Lists.newArrayList(original.getParts()));
    }



    public HttpRequest(Struct requestAsstruct) {
        this.url = requestAsstruct.getString(URL);
        Preconditions.checkNotNull(url, "'url' is required");

        Map<String, List<String>> headers = requestAsstruct.getMap(HEADERS);
        if (headers != null && !headers.isEmpty()) {
            this.headers = headers;
        }else{
            this.headers = Maps.newHashMap();
        }

        this.method = HttpRequest.Method.valueOf(requestAsstruct.getString(METHOD).toUpperCase());
        Preconditions.checkNotNull(method, "'method' is required");

        this.bodyType = BodyType.valueOf(Optional.ofNullable(requestAsstruct.getString(BODY_TYPE)).orElse(BodyType.STRING.name()));

        this.bodyAsByteArray = requestAsstruct.getString(BODY_AS_BYTE_ARRAY);
        this.bodyAsString = requestAsstruct.getString(BODY_AS_STRING);
        this.bodyAsForm = requestAsstruct.getMap(BODY_AS_FORM);

        List<Struct> structs = requestAsstruct.getArray(PARTS);
        if (structs != null) {
            //this is a multipart request
            for (Struct struct : structs) {
                HttpPart httpPart = new HttpPart(struct);
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

    public BodyType getBodyType(){
        return bodyType;
    }

    public List<HttpPart> getParts() {
        return parts;
    }

    public void setParts(List<HttpPart> httpParts) {
        this.parts = httpParts;
        if(parts!=null && !parts.isEmpty()){
            this.bodyType = BodyType.MULTIPART;
        }
    }

    public void addPart(HttpPart httpPart) {
        parts.add(httpPart);
    }

    @JsonIgnore
    public String getContentType(){
        if(headers != null
                && headers.containsKey(CONTENT_TYPE)
                &&headers.get(CONTENT_TYPE)!=null
                &&!headers.get(CONTENT_TYPE).isEmpty()){
            return headers.get(CONTENT_TYPE).get(0);
        }
        return null;
    }

    @JsonIgnore
    public String getBoundary(){
        String contentType = getContentType();
        String boundary = null;
        if(contentType!=null){
            Matcher matcher = BOUNDARY.matcher(contentType);
            if(matcher.matches()){
                boundary = matcher.group(1);
            }
        }
        return boundary;
    }
    public void setContentType(String contentType){
        headers.put(CONTENT_TYPE,Lists.newArrayList(contentType));
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRequest that = (HttpRequest) o;
        return url.equals(that.url)
                && Objects.equals(headers, that.headers)
                && method.equals(that.method)
                && Objects.equals(parts, that.parts)
                && Objects.equals(bodyAsByteArray, that.bodyAsByteArray)
                && Objects.equals(bodyAsForm, that.bodyAsForm)
                && Objects.equals(bodyAsString, that.bodyAsString)
                ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, headers, method, parts,bodyAsByteArray,bodyAsForm,bodyAsString);
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
                ", parts=" + parts +
                ", bodyType=" + bodyType +
                '}';
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(URL, this.getUrl())
                .put(HEADERS, this.getHeaders())
                .put(METHOD, this.getMethod().name())
                .put(BODY_TYPE,this.getBodyType().name())
                .put(PARTS, this.getParts().stream().map(HttpPart::toStruct).collect(Collectors.toList()))
                ;
    }

    public void setBodyAsString(String bodyAsString) {
        this.bodyAsString = bodyAsString;
        this.bodyType = BodyType.STRING;
    }

    public void setBodyAsByteArray(byte[] content) {
        if(content!=null && content.length>0) {
            bodyAsByteArray = Base64.getEncoder().encodeToString(content);
            bodyType = BodyType.BYTE_ARRAY;
        }
        //if no Content-Type is set, we set the default application/octet-stream
        if(headers!=null && doesNotContainHeader(CONTENT_TYPE)){
            headers.put(CONTENT_TYPE,Lists.newArrayList("application/octet-stream"));
        }
    }

    private boolean doesNotContainHeader(String key) {
        return headers.keySet().stream().filter(k -> k.equalsIgnoreCase(key)).findAny().isEmpty();
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
        bodyType = BodyType.FORM;
        if(headers!=null && doesNotContainHeader(CONTENT_TYPE)){
            headers.put(CONTENT_TYPE,Lists.newArrayList("application/x-www-form-urlencoded"));
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
