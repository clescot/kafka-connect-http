package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.File;
import java.net.URI;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * part of a multipart request.
 */
@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpPart.SCHEMA_AS_STRING,
        refs = {})
@JsonInclude(Include.NON_NULL)
public class HttpPart {
    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String CONTENT_TYPE = "Content-Type";
    private URI fileUri;
    private HttpRequest.BodyType bodyType;
    private Map<String, List<String>> headers = Maps.newHashMap();
    private String contentAsString;
    private String contentAsByteArray;
    //Tuple2<parameterName,Tuple2<parameterValue,Optional<File>>
    private Map.Entry<String, Map.Entry<String, Optional<File>>> contentAsFormEntry;
    public static final int VERSION = 1;
    public static final String HEADERS = "headers";
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_FORM_DATA = "bodyAsFormData";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";
    public static final String FILE_URI = "fileUri";

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpPart.class.getName())
            .version(VERSION)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).optional().build())
            .field(BODY_TYPE, Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM_DATA, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build()).optional().schema())
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FILE_URI, Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .schema();
    public static final String SCHEMA_ID = HttpExchange.BASE_SCHEMA_ID + VERSION + "/" + "http-part.json";
    public static final String SCHEMA_AS_STRING =
            "{\n" +
                    "  \"$id\": \"" + SCHEMA_ID + "\",\n" +
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
                    "    \"bodyAsFormData\": {\n" +
                    "      \"type\": \"string\"\n" +
                    "    },\n" +
                    "    \"bodyAsByteArray\": {\n" +
                    "      \"type\": \"string\"\n" +
                    "    },\n" +
                    "    \"fileURI\": {\n" +
                    "      \"type\": \"string\"\n" +
                    "    },\n" +
                    "    \"bodyType\": {\n" +
                    "      \"type\": \"string\",\n" +
                    "      \"enum\": [\n" +
                    "        \"STRING\",\n" +
                    "        \"FORM_DATA\",\n" +
                    "        \"BYTE_ARRAY\"\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"required\": [\n" +
                    "    \"bodyType\"\n" +
                    "  ]\n" +
                    "}";


    //for deserialization only
    protected HttpPart() {
    }

    //content as byte array
    public HttpPart(Map<String, List<String>> headers, byte[] contentAsByteArray) {
        this.bodyType = HttpRequest.BodyType.BYTE_ARRAY;
        this.headers = headers;
        this.contentAsByteArray = Base64.getMimeEncoder().encodeToString(contentAsByteArray);
    }

    //content as byte array without headers
    public HttpPart(byte[] contentAsByteArray) {
        this(Map.of(CONTENT_TYPE, Lists.newArrayList(APPLICATION_OCTET_STREAM)), contentAsByteArray);
    }

    //content as form data with plain file content
    public HttpPart(Map<String, List<String>> headers, String parameterName,String parameterValue,File file) {
        this.bodyType = HttpRequest.BodyType.FORM_DATA;
        this.headers = headers;
        this.contentAsFormEntry = Map.entry(parameterName,Map.entry(parameterValue,Optional.ofNullable(file)));
    }

    //content as form data with file content as a reference
    public HttpPart(Map<String, List<String>> headers, String parameterName, String parameterValue, URI fileUri) {
        this.bodyType = HttpRequest.BodyType.FORM_DATA;
        this.headers = headers;
        this.contentAsFormEntry = Map.entry(parameterName,Map.entry(parameterValue,Optional.empty()));
        this.fileUri = fileUri;
    }
    //content as form data with plain file content without headers
    public HttpPart(String parameterName,String parameterValue,File file) {
        this(Map.of(CONTENT_TYPE, Lists.newArrayList(APPLICATION_X_WWW_FORM_URLENCODED)), parameterName,parameterValue,file);
    }

    //content as string
    public HttpPart(Map<String, List<String>> headers, String contentAsString) {
        this.bodyType = HttpRequest.BodyType.STRING;
        this.headers = headers;
        this.contentAsString = contentAsString;
    }
    //content as string without headers
    public HttpPart(String contentAsString) {
        this(Map.of(CONTENT_TYPE, Lists.newArrayList(APPLICATION_JSON)), contentAsString);
    }

    //for serialization
    public HttpPart(Struct struct) {
        this.headers = struct.getMap(HEADERS);
        this.contentAsByteArray = struct.getString(BODY_AS_BYTE_ARRAY);
    }

    public HttpRequest.BodyType getBodyType() {
        return bodyType;
    }


    public String getContentAsString() {
        return contentAsString;
    }


    public void setContentAsByteArray(byte[] contentAsByteArray) {
        if (contentAsByteArray != null) {
            this.contentAsByteArray = Base64.getEncoder().encodeToString(contentAsByteArray);
        }
    }

    public void setContentAsString(String contentAsString) {
        this.contentAsString = contentAsString;
    }

    public void setContentAsFormEntry(Map.Entry<String, Map.Entry<String, Optional<File>>> contentAsFormEntry) {
        this.contentAsFormEntry = contentAsFormEntry;
    }

    public Map.Entry<String, Map.Entry<String, Optional<File>>> getContentAsFormEntry() {
        return contentAsFormEntry;
    }




    public byte[] getContentAsByteArray() {
        if (contentAsByteArray != null) {
            return Base64.getMimeDecoder().decode(contentAsByteArray);
        }
        return null;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        Preconditions.checkArgument(headers.keySet().stream().allMatch(key ->
                        "Content-Disposition".equalsIgnoreCase(key) ||
                                "Content-Type".equalsIgnoreCase(key) ||
                                "Content-Transfer-Encoding".equalsIgnoreCase(key)),
                "all headers key in a multipart request must be 'Content-Disposition','Content-Type', " +
                        "or 'Content-Transfer-Encoding'. current Headers key of this part are : "
                        + Joiner.on(",").join(headers.keySet()));
        this.headers = headers;
    }

    @JsonIgnore
    public String getContentType() {
        if (headers != null
                && headers.containsKey(CONTENT_TYPE)
                && headers.get(CONTENT_TYPE) != null
                && !headers.get(CONTENT_TYPE).isEmpty()) {
            return headers.get("Content-Type").get(0);
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HttpPart)) return false;
        HttpPart httpPart = (HttpPart) o;
        return bodyType == httpPart.bodyType
                && Objects.equals(headers, httpPart.headers)
                && Objects.equals(contentAsString, httpPart.contentAsString)
                && Objects.deepEquals(contentAsByteArray, httpPart.contentAsByteArray)
                && Objects.equals(contentAsFormEntry, httpPart.contentAsFormEntry)
                && Objects.equals(fileUri, httpPart.fileUri)
                ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bodyType, headers,contentAsString, contentAsByteArray, contentAsFormEntry,fileUri);
    }

    @Override
    public String toString() {
        return "Part{" +
                "bodyType:\"" + bodyType +
                "\", headers:" + headers +
                ", \"contentAsString\":" + contentAsString + '\"' +
                ", \"contentAsByteArray\":\"" + contentAsByteArray + "\"" +
                ", \"contentAsForm\":\"" + contentAsFormEntry + "\"" +
                ", \"fileUri\":\"" + fileUri + "\"" +
                '}';
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA);
        struct.put(HEADERS, getHeaders());
        struct.put(BODY_TYPE, getBodyType().name());
        struct.put(BODY_AS_STRING, contentAsString);
        struct.put(BODY_AS_FORM_DATA, contentAsFormEntry);
        struct.put(BODY_AS_BYTE_ARRAY, contentAsByteArray);
        struct.put(FILE_URI, fileUri);
        return struct;
    }


}
