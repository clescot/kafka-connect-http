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
import java.io.Serializable;
import java.net.URI;
import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;
import static io.github.clescot.kafka.connect.http.core.HttpPart.BodyType.FORM_DATA;

/**
 * part of a multipart request.
 */
@JsonInclude(Include.NON_NULL)
public class HttpPart implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;
    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String CONTENT_TYPE = "Content-Type";
    private URI fileUri;
    private HttpPart.BodyType bodyType;
    private Map<String, List<String>> headers = Maps.newHashMap();
    private String contentAsString;
    private String contentAsByteArray;
    //Map.Entry<parameterName,Map.Entry<parameterValue,Optional<File>>
    private Map.Entry<String, File> contentAsFormEntry;
    public static final int VERSION = 2;
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


    //for deserialization only
    @SuppressWarnings("unused")
    protected HttpPart() {
    }

    //content as byte array
    public HttpPart(Map<String, List<String>> headers, byte[] contentAsByteArray) {
        this.bodyType = HttpPart.BodyType.BYTE_ARRAY;
        this.headers = headers!=null?headers:Maps.newHashMap();
        this.contentAsByteArray = Base64.getMimeEncoder().encodeToString(contentAsByteArray);
    }

    //content as byte array without headers
    public HttpPart(byte[] contentAsByteArray) {
        this(Map.of(CONTENT_TYPE, Lists.newArrayList(APPLICATION_OCTET_STREAM)), contentAsByteArray);
    }

    //content as form data with plain file content
    public HttpPart(Map<String, List<String>> headers, String fileName,File file) {
        this.bodyType = FORM_DATA;
        this.headers = headers!=null?headers:Maps.newHashMap();
        this.contentAsFormEntry = Map.entry(fileName,file);
    }

    //content as form data with plain file content without headers
    public HttpPart(String fileName,File file) {
        this(Map.of(CONTENT_TYPE, Lists.newArrayList(APPLICATION_X_WWW_FORM_URLENCODED)),fileName,file);
    }

    //content as form data with file content as a reference
    public HttpPart(Map<String, List<String>> headers, String fileName, URI fileUri) {
        this.bodyType = BodyType.FORM_DATA_AS_REFERENCE;
        this.headers = headers!=null?headers:Maps.newHashMap();
        this.contentAsFormEntry = Map.entry(fileName,new File(fileUri));
        this.fileUri = fileUri;
    }


    //content as form data with file content as a reference without headers
    public HttpPart(String fileName, URI fileUri) {
        this(Map.of(CONTENT_TYPE, Lists.newArrayList(APPLICATION_X_WWW_FORM_URLENCODED)),  fileName, fileUri);
    }

    //content as string
    public HttpPart(Map<String, List<String>> headers, String contentAsString) {
        this.bodyType = HttpPart.BodyType.STRING;
        this.headers = headers!=null?headers:Maps.newHashMap();
        this.contentAsString = contentAsString;
    }
    //content as string without headers
    public HttpPart(String contentAsString) {
        this(Map.of(CONTENT_TYPE, Lists.newArrayList(APPLICATION_JSON)), contentAsString);
    }

    //for serialization
    public HttpPart(Struct struct) {
        this.headers = struct.getMap(HEADERS)!=null?struct.getMap(HEADERS): Maps.newHashMap();
        this.bodyType = HttpPart.BodyType.valueOf(struct.getString(BODY_TYPE));
        this.contentAsByteArray = struct.getString(BODY_AS_BYTE_ARRAY);
        this.contentAsString = struct.getString(BODY_AS_STRING);
        //this.contentAsFormEntry = struct.getMap(BODY_AS_FORM_DATA);
    }

    public HttpPart.BodyType getBodyType() {
        return bodyType;
    }


    public String getContentAsString() {
        return contentAsString;
    }

    public URI getFileUri() {
        return fileUri;
    }

    public void setContentAsByteArray(byte[] contentAsByteArray) {
        if (contentAsByteArray != null) {
            this.contentAsByteArray = Base64.getEncoder().encodeToString(contentAsByteArray);
        }
    }

    public void setContentAsString(String contentAsString) {
        this.contentAsString = contentAsString;
    }

    public void setContentAsFormEntry(Map.Entry<String, File> contentAsFormEntry) {
        this.contentAsFormEntry = contentAsFormEntry;
    }

    public Map.Entry<String,File> getContentAsFormEntry() {
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
        return "HttpPart{" +
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

    @Override
    protected Object clone() throws CloneNotSupportedException {
        HttpPart clone = (HttpPart) super.clone();
        if(getHeaders()!=null) {
            clone.setHeaders(new HashMap<>(getHeaders()));
        }
        if (getContentAsFormEntry() != null) {
            clone.setContentAsFormEntry(Map.entry(
                    getContentAsFormEntry().getKey(),
                            getContentAsFormEntry().getValue()));
        }
        if(getContentAsString()!=null) {
            clone.setContentAsString(getContentAsString());
        }
        if(getFileUri()!=null){
            clone.fileUri = getFileUri();
        }
        if (getContentAsByteArray()!=null) {
            clone.setContentAsByteArray(getContentAsByteArray());
        }
        clone.bodyType = getBodyType();
        return clone;
    }

    @JsonIgnore
    public long getBodyContentLength() {
        switch(bodyType) {
            case STRING:
                return contentAsString != null ? contentAsString.length() : 0;
            case BYTE_ARRAY:
                return contentAsByteArray != null ? getContentAsByteArray().length : 0;
            case FORM_DATA:
            case FORM_DATA_AS_REFERENCE:
                return contentAsFormEntry != null && contentAsFormEntry.getValue() != null ? contentAsFormEntry.getValue().length() : 0;
            default:
                return 0;
        }
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


    public enum BodyType {
        STRING,
        BYTE_ARRAY,
        FORM_DATA,
        FORM_DATA_AS_REFERENCE
    }

}
