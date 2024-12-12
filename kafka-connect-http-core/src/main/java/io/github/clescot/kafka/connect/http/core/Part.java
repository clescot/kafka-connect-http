package io.github.clescot.kafka.connect.http.core;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Part {
    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String CONTENT_TYPE = "Content-Type";
    private final HttpRequest.BodyType bodyType;
    private Map<String,List<String>> headers = Maps.newHashMap();
    private String contentType;
    private String contentAsString;
    private byte[] contentAsByteArray;
    private Map<String, String> contentAsForm;
    public static final int VERSION = 1;
    public static final String HEADERS = "headers";
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_FORM = "bodyAsForm";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(Part.class.getName())
            .version(VERSION)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).build())
            .field(BODY_TYPE,Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_BYTES_SCHEMA)
            .schema();


    public Part(byte[] contentAsByteArray) {
        this.bodyType = HttpRequest.BodyType.BYTE_ARRAY;
        headers.putIfAbsent(CONTENT_TYPE, Lists.newArrayList(APPLICATION_OCTET_STREAM));
        this.contentAsByteArray = contentAsByteArray;
    }

    public Part(Map<String,String> contentAsForm) {
        this.bodyType = HttpRequest.BodyType.FORM;
        headers.putIfAbsent(CONTENT_TYPE, Lists.newArrayList(APPLICATION_X_WWW_FORM_URLENCODED));
        this.contentAsForm = contentAsForm;
    }

    public Part(String contentAsString) {
        this.bodyType = HttpRequest.BodyType.STRING;
        headers.putIfAbsent(CONTENT_TYPE, Lists.newArrayList(APPLICATION_JSON));
        this.contentAsString = contentAsString;
    }

    public HttpRequest.BodyType getBodyType() {
        return bodyType;
    }


    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getContentAsString(){
        return contentAsString;
    }


    public void setContentAsByteArray(byte[] contentAsByteArray) {
        this.contentAsByteArray = contentAsByteArray;
    }

    public void setContentAsString(String contentAsString) {
        this.contentAsString = contentAsString;
    }

    public void setContentAsForm(Map<String, String> contentAsForm) {
        this.contentAsForm = contentAsForm;
    }

    public Map<String,String> getContentAsForm(){
        return contentAsForm;
    }

    public byte[] getContentAsByteArray() {
        return contentAsByteArray;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Part)) return false;
        Part part = (Part) o;
        return bodyType == part.bodyType && Objects.equals(contentType, part.contentType) && Objects.equals(contentAsString, part.contentAsString) && Objects.deepEquals(contentAsByteArray, part.contentAsByteArray) && Objects.equals(contentAsForm, part.contentAsForm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bodyType, contentType, contentAsString, Arrays.hashCode(contentAsByteArray), contentAsForm);
    }

    @Override
    public String toString() {
        return "Part{" +
                "bodyType=" + bodyType +
                ", contentType='" + contentType + '\'' +
                ", stringContent='" + contentAsString + '\'' +
                ", byteContent=" + Arrays.toString(contentAsByteArray) +
                ", formContent=" + contentAsForm +
                '}';
    }

    public Struct toStruct(){
        Struct struct = new Struct(SCHEMA);
        struct.put(HEADERS,getHeaders());
        struct.put(BODY_TYPE,getBodyType().name());
        struct.put(BODY_AS_STRING,getContentAsString());
        struct.put(BODY_AS_FORM,getContentAsForm());
        struct.put(BODY_AS_BYTE_ARRAY,getContentAsByteArray());
        return struct;
    }


}
