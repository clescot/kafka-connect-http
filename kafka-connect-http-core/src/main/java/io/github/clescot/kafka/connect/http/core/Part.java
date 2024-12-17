package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.*;

/**
 * part of a multi-part request.
 */
@io.confluent.kafka.schemaregistry.annotations.Schema(value =Part.JSON_SCHEMA,
        refs = {})
@JsonInclude(Include.NON_NULL)
public class Part {
    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String CONTENT_TYPE = "Content-Type";
    private HttpRequest.BodyType bodyType;
    private Map<String,List<String>> headers = Maps.newHashMap();
    private String contentAsString;
    private String contentAsByteArray;
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
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).optional().build())
            .field(BODY_TYPE,Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_BYTES_SCHEMA)
            .optional()
            .schema();
    public static final String SCHEMA_ID = HttpExchange.BASE_SCHEMA_ID+"http-part.json";
    public static final String JSON_SCHEMA =
            "{\n" +
            "  \"$id\": \"https://raw.githubusercontent.com/clescot/kafka-connect-http/master/kafka-connect-http-core/src/main/resources/schemas/json/versions/"+SCHEMA_ID+"\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema#\",\n" +
            "  \"title\": \"Http Part\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"additionalProperties\": false,\n" +
            "  \"properties\": {\n" +
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
            "        \"BYTE_ARRAY\"\n" +
            "      ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"bodyType\"\n" +
            "  ]\n" +
            "}";


    //for deserialization only
    protected Part(){}
    public Part(byte[] contentAsByteArray) {
        this.bodyType = HttpRequest.BodyType.BYTE_ARRAY;
        headers.putIfAbsent(CONTENT_TYPE, Lists.newArrayList(APPLICATION_OCTET_STREAM));
        this.contentAsByteArray = Base64.getMimeEncoder().encodeToString(contentAsByteArray);
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

    public Part(Struct struct){
        this.headers =  struct.getMap(HEADERS);
        this.contentAsByteArray = struct.getString(BODY_AS_BYTE_ARRAY);
    }

    public HttpRequest.BodyType getBodyType() {
        return bodyType;
    }



    public String getContentAsString(){
        return contentAsString;
    }


    public void setContentAsByteArray(byte[] contentAsByteArray) {
        if(contentAsByteArray!=null) {
            this.contentAsByteArray = Base64.getEncoder().encodeToString(contentAsByteArray);
        }
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
        if(contentAsByteArray!=null) {
            return Base64.getMimeDecoder().decode(contentAsByteArray);
        }
        return null;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        Preconditions.checkArgument(headers.keySet().stream().allMatch(key->
                "Content-Disposition".equalsIgnoreCase(key)||
                "Content-Type".equalsIgnoreCase(key)||
                "Content-Transfer-Encoding".equalsIgnoreCase(key)),
                "all headers key in a multipart request must be 'Content-Disposition','Content-Type', " +
                        "or 'Content-Transfer-Encoding'. current Headers key of this part are : "
                        + Joiner.on(",").join(headers.keySet()));
        this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Part)) return false;
        Part part = (Part) o;
        return bodyType == part.bodyType
                && Objects.equals(contentAsString, part.contentAsString)
                && Objects.deepEquals(contentAsByteArray, part.contentAsByteArray)
                && Objects.equals(contentAsForm, part.contentAsForm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bodyType, contentAsString, contentAsByteArray, contentAsForm);
    }

    @Override
    public String toString() {
        return "Part{" +
                "bodyType:\"" + bodyType +
                ", \"contentAsString\":" + contentAsString + '\"' +
                ", \"contentAsByteArray\":\"" + contentAsByteArray +"\""+
                ", \"contentAsForm\":\"" + contentAsForm +"\""+
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
