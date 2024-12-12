package io.github.clescot.kafka.connect.http.core;

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

import org.apache.kafka.connect.data.Struct;

import static io.github.clescot.kafka.connect.http.core.Part.*;

@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpRequest.SCHEMA_AS_STRING,
        refs = {})
public class HttpRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRequest.class);
    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";
    public static final String PARTS = "parts";
    public static final String MULTIPART_MIMETYPE = "multipartMimeType";
    public static final String MULTIPART_BOUNDARY = "multipartBoundary";

    public static final int VERSION = 2;

    //request
    @JsonProperty(required = true)
    private String url;
    @JsonProperty
    private Map<String, List<String>> headers = Maps.newHashMap();
    @JsonProperty(defaultValue = "GET")
    private HttpRequest.Method method;
    @JsonProperty
    private String multipartBoundary=UUID.randomUUID().toString();
    @JsonProperty(defaultValue = "multipart/form-data")
    private String multipartMimeType;
    private List<Part> parts = Lists.newArrayList();

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpRequest.class.getName())
            .version(VERSION)
            //request
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).build())
            .field(URL, Schema.STRING_SCHEMA)
            .field(METHOD, Schema.STRING_SCHEMA)
            .field(PARTS,SchemaBuilder.array(Part.SCHEMA).build())
            .field(MULTIPART_MIMETYPE,Schema.OPTIONAL_STRING_SCHEMA)
            .field(MULTIPART_BOUNDARY,Schema.OPTIONAL_STRING_SCHEMA)
            .schema();
    public static final String SCHEMA_ID = HttpExchange.BASE_SCHEMA_ID+"http-request.json";
    public static final String SCHEMA_AS_STRING = "{\n" +
            "  \"$id\": \"" + SCHEMA_ID + "\",\n" +
            "\"$schema\": \"http://json-schema.org/draft/2019-09/schema#\",\n" +
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
            "    \"bodyAsString\":\n" +
            "    {\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  ,\n" +
            "    \"bodyAsForm\":\n" +
            "    {\n" +
            "      \"type\": \"object\",\n" +
            "      \"connect.type\": \"map\",\n" +
            "      \"additionalProperties\": { \"type\": \"string\" }\n" +
            "    }\n" +
            "  ,\n" +
            "    \"bodyAsByteArray\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"bodyAsMultipart\": {\n" +
            "      \"type\": \"array\",\n" +
            "      \"items\": {\n" +
            "        \"type\": \"string\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"multipartBoundary\":  {\n" +
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
    protected HttpRequest() {}
    public HttpRequest(String url){
        this(url,HttpRequest.Method.GET,null,null);
    }
    public HttpRequest(String url,
                       HttpRequest.Method method){
        this(url,method,null,null);
    }
    public HttpRequest(String url,
                       HttpRequest.Method method,
                       String multiPartMimeType,
                       String multipartBoundary
                       ) {
        Preconditions.checkNotNull(url, "url is required");
        this.url = url;
        this.method = method;
        this.multipartMimeType = multiPartMimeType;
        this.multipartBoundary = multipartBoundary;
    }

    public HttpRequest(HttpRequest original){
            this(original.getUrl(),original.getMethod(),original.getMultipartMimeType(),original.getMultipartBoundary());
            this.setHeaders(Maps.newHashMap(original.getHeaders()));
            this.setParts(original.getParts());
            this.setMultipartMimeType(original.getMultipartMimeType());
            this.setMultipartBoundary(original.getMultipartBoundary());
    }
    public HttpRequest(Struct struct){
        this.url = struct.getString(URL);
        Preconditions.checkNotNull(url, "'url' is required");

        Map<String, List<String>> headers = struct.getMap(HEADERS);
        if(headers!=null&&!headers.isEmpty()) {
            this.headers = headers;
        }

        this.method = HttpRequest.Method.valueOf(struct.getString(METHOD).toUpperCase());
        Preconditions.checkNotNull(method, "'method' is required");

        this.multipartMimeType = struct.getString(MULTIPART_MIMETYPE);
        this.multipartBoundary = struct.getString(MULTIPART_BOUNDARY);
        this.parts = struct.getArray(PARTS);

    }

    public String getMultipartMimeType() {
        return multipartMimeType;
    }

    public void setMultipartMimeType(String multipartMimeType) {
        this.multipartMimeType = multipartMimeType;
    }

    public List<Part> getParts() {
        return parts;
    }

    public void setParts(List<Part> parts) {
        this.parts = parts;
    }

    public void addPart(Part part){
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
                && Objects.equals(multipartMimeType,that.multipartMimeType)
                && Objects.equals(multipartBoundary, that.multipartBoundary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, headers, method,parts,multipartMimeType, multipartBoundary);
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "headers=" + headers +
                ", url='" + url + '\'' +
                ", method=" + method +
                ", multipartMimeType=" + multipartMimeType +
                ", multipartBoundary='" + multipartBoundary + '\'' +
                ", parts=" + parts +
                '}';
    }



    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(URL, this.getUrl())
                .put(HEADERS, this.getHeaders())
                .put(METHOD, this.getMethod().name())
                .put(PARTS,this.getParts())
                .put(MULTIPART_MIMETYPE,this.getMultipartMimeType())
                .put(MULTIPART_BOUNDARY,this.getMultipartBoundary())
                ;
    }

    public void setBodyAsString(String bodyAsString) {
        setBodyAsString(APPLICATION_JSON,bodyAsString);
    }
    public void setBodyAsString(String mimeType,String bodyAsString) {
        if(mimeType==null||mimeType.isBlank()){
            mimeType = "application/json";
        }
        if(parts==null){
            parts = Lists.newArrayList();
        }
        if(parts.isEmpty()){
            parts.add(new Part(mimeType,bodyAsString));
        }
        if(parts.size()==1){
            parts.get(0).setContentAsString(bodyAsString);
        }else{
            //parts has more than one part
            throw new IllegalArgumentException("you cannot set a body to a multipart request. add a part instead");
        }
    }

    public void setBodyAsByteArray(byte[] content) {
        setBodyAsByteArray(APPLICATION_OCTET_STREAM,content);
    }
    public void setBodyAsByteArray(String contentType,byte[] content) {
        if(parts==null){
            parts = Lists.newArrayList();
        }
        if(parts.isEmpty()){
            parts.add(new Part(contentType,content));
        }
        if(parts.size()==1){
            parts.get(0).setContentAsByteArray(content);
        }else{
            //parts has more than one part
            throw new IllegalArgumentException("you cannot set a body to a multipart request. add a part instead");
        }
    }

    public byte[] getBodyAsByteArray(){
        if(parts==null||parts.isEmpty()){
            return new byte[]{};
        }else if(parts.size()==1){
            Part part = parts.get(0);
            if(BodyType.BYTE_ARRAY.equals(part.getBodyType())){
                return part.getContentAsByteArray();
            }else{
                throw new IllegalStateException("the bodyType is not BYTE_ARRAY but '"+part.getBodyType()+"'");
            }
        }else{
            throw new IllegalStateException("this is a multipart request");
        }
    }

    public void setBodyAsForm(Map<String, String> form) {
        setBodyAsForm(APPLICATION_X_WWW_FORM_URLENCODED,form);
    }

    public void setBodyAsForm(String contentType, Map<String, String> form) {
        if(parts==null){
            parts = Lists.newArrayList();
        }
        if(parts.isEmpty()){
            parts.add(new Part(contentType,form));
        }
        if(parts.size()==1){
            parts.get(0).setContentAsForm(form);
        }else{
            //parts has more than one part
            throw new IllegalArgumentException("you cannot set a body to a multipart request. add a part instead");
        }
    }

    public String getBodyAsString() {
        if(parts==null||parts.isEmpty()){
            return null;
        }else if(parts.size()==1){
            Part part = parts.get(0);
            if(BodyType.STRING.equals(part.getBodyType())){
                return part.getContentAsString();
            }else{
                throw new IllegalStateException("the bodyType is not BYTE_ARRAY but '"+part.getBodyType()+"'");
            }
        }else{
            throw new IllegalStateException("this is a multipart request");
        }
    }

    public BodyType getBodyType() {
        if(parts==null||parts.isEmpty()){
            return BodyType.STRING;
        }else if(parts.size()>1){
            return BodyType.MULTIPART;
        }else{
            return parts.get(0).getBodyType();
        }

    }

    public Map<String, String> getBodyAsForm() {
        if(parts==null||parts.isEmpty()){
            return null;
        }else if(parts.size()==1){
            Part part = parts.get(0);
            if(BodyType.FORM.equals(part.getBodyType())){
                return part.getContentAsForm();
            }else{
                throw new IllegalStateException("the bodyType is not BYTE_ARRAY but '"+part.getBodyType()+"'");
            }
        }else{
            throw new IllegalStateException("this is a multipart request");
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
