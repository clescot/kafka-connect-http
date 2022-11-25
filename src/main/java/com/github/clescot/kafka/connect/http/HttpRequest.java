package com.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@io.confluent.kafka.schemaregistry.annotations.Schema(value = "{\n" +
        "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema#\",\n" +
        "  \"title\": \"Http Request\",\n" +
        "  \"type\": \"object\",\n" +
        "  \"additionalProperties\": false,\n" +
        "  \"required\": [\"url\",\"method\"],\n" +
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
        "          \"type\": \"string\"\n" +
        "    },\n" +
        "    \"bodyAsByteArray\":       {\n" +
        "          \"type\": \"string\"\n" +
        "    },\n" +
        "    \"bodyAsMultipart\":    {\n" +
        "          \"type\": \"array\",\n" +
        "          \"items\": {\n" +
        "            \"type\": \"string\"\n" +
        "          }\n" +
        "    },\n" +
        "    \"bodyType\":   {\n" +
        "          \"type\": \"string\",\n" +
        "          \"enum\": [\n" +
        "            \"STRING\",\n" +
        "            \"BYTE_ARRAY\",\n" +
        "            \"MULTIPART\"\n" +
        "          ]\n" +
        "    }\n" +
        "  },\n" +
        "  \"required\": [\n" +
        "    \"url\",\n" +
        "    \"method\"\n" +
        "  ]\n" +
        "}",
        refs = {})
public class HttpRequest {


    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";

    //only one 'body' field must be set
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";
    public static final String BODY_AS_MULTIPART = "bodyAsMultipart";
    public static final int VERSION = 1;

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpRequest.class);


    //request
    @JsonProperty(required = true)
    private String url;
    @JsonProperty
    private Map<String, List<String>> headers = Maps.newHashMap();
    @JsonProperty(required = true)
    private String method;
    @JsonProperty
    private String bodyAsString="";
    @JsonProperty
    private String bodyAsByteArray="";
    @JsonProperty
    private List<String> bodyAsMultipart = Lists.newArrayList();
    @JsonProperty
    private BodyType bodyType;


    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpRequest.class.getName())
            .version(VERSION)
            //request
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(URL, Schema.STRING_SCHEMA)
            .field(METHOD, Schema.STRING_SCHEMA)
            .field(BODY_TYPE, Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_MULTIPART, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA));

    public HttpRequest(String url,
                       String method,
                       String bodyType,
                       @Nullable String bodyAsString,
                       @Nullable byte[] bodyAsByteArray,
                       @Nullable List<byte[]> bodyAsMultipart) {
        this.url = url;
        this.method = method;
        this.bodyType = BodyType.valueOf(bodyType);
        this.bodyAsString = bodyAsString;
        this.bodyAsByteArray = bodyAsByteArray != null ?  Base64.getEncoder().encodeToString(bodyAsByteArray) : this.bodyAsByteArray;
        this.bodyAsMultipart = bodyAsMultipart != null ? convertMultipart(bodyAsMultipart) : this.bodyAsMultipart;

        if (BodyType.STRING==this.bodyType){
               if(!bodyAsString.isEmpty() &&this.bodyAsByteArray.isEmpty()&& this.bodyAsMultipart.isEmpty()){
                   LOGGER.trace("bodyType 'STRING' is accurate against bodyAsString,bodyAsByteArray and bodyAsMultipart fields");
               }else{
                   LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
                   throw new IllegalArgumentException("when bodyType is set to 'STRING', the 'bodyAsString' field must be non null ; 'bodyAsByteArray' and 'bodyAsMultipart' fields must be null ");
               }
        } else if(BodyType.BYTE_ARRAY.equals(this.bodyType)){
            if(bodyAsString.isEmpty() && !this.bodyAsByteArray.isEmpty() && this.bodyAsMultipart.isEmpty()){
                LOGGER.trace("bodyType 'BYTE_ARRAY' is accurate against bodyAsString,bodyAsByteArray and bodyAsMultipart fields");
            }else{
                LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
                throw new IllegalArgumentException("when bodyType is set to 'BYTE_ARRAY', the 'bodyAsString' and 'bodyAsMultipart' fields must be null ; 'bodyAsByteArray'  must be non-null");
            }
        }else if(BodyType.MULTIPART.equals(this.bodyType)){
            if(bodyAsString.isEmpty() && this.bodyAsByteArray.isEmpty() && this.bodyAsMultipart.size()>0){
                LOGGER.trace("bodyType 'MULTIPART' is accurate against bodyAsString,bodyAsByteArray and bodyAsMultipart fields");
            }else{
                LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
                throw new IllegalArgumentException("when 'bodyType' is set to 'MULTIPART', the 'bodyAsString' and 'bodyAsByteArray' fields must be null ; 'bodyAsMultipart' must be non-null");
            }
        }else{
            LOGGER.error("bodyType is set to {}. bodyAsString:{},bodyAsByteArray:{},bodyAsMultipart:{}",bodyType,bodyAsString,bodyAsByteArray,bodyAsMultipart);
            throw new IllegalArgumentException("'bodyType' is not set to either 'STRING','BYTE_ARRAY', or 'MULTIPART'");
        }
    }

    private List<String> convertMultipart(List<byte[]> bodyAsMultipart) {
        List<String> results = Lists.newArrayList();
        for (byte[] bytes : bodyAsMultipart) {
            results.add(Base64.getEncoder().encodeToString(bytes));
        }
        return results;
    }


    /**
     * only for json deserialization
     */
    protected HttpRequest() {
    }

    public void setBodyType(BodyType bodyType) {
        this.bodyType = bodyType;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public String getUrl() {
        return url;
    }

    public String getMethod() {
        return method;
    }

    public String getBodyAsString() {
        return bodyAsString;
    }

    public byte[] getBodyAsByteArray() {
        return this.bodyAsByteArray!=null?Base64.getDecoder().decode(bodyAsByteArray):new byte[0];
    }

    public List<byte[]> getBodyAsMultipart() {
        List<byte[]> multipart = Lists.newArrayList();
        for (String encodedPart : this.bodyAsMultipart) {
            multipart.add(Base64.getDecoder().decode(encodedPart));
        }
        return multipart;
    }

    public BodyType getBodyType() {
        return bodyType;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRequest that = (HttpRequest) o;
        return url.equals(that.url) && Objects.equals(headers, that.headers) && method.equals(that.method) && Objects.equals(bodyAsString, that.bodyAsString) && Objects.equals(bodyAsByteArray, that.bodyAsByteArray) && Objects.equals(bodyAsMultipart, that.bodyAsMultipart) && bodyType == that.bodyType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, headers, method, bodyAsString, bodyAsByteArray, bodyAsMultipart, bodyType);
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "url='" + url + '\'' +
                ", headers=" + headers +
                ", method='" + method + '\'' +
                ", bodyAsString='" + bodyAsString + '\'' +
                ", bodyAsByteArray='" + bodyAsByteArray + '\'' +
                ", bodyAsMultipart=" + bodyAsMultipart +
                ", bodyType=" + bodyType +
                '}';
    }

    public Struct toStruct() {
        return  new Struct(SCHEMA)
                        .put(URL, url)
                        .put(HEADERS, headers)
                        .put(METHOD, method)
                        .put(BODY_TYPE, bodyType.name())
                        .put(BODY_AS_STRING, bodyAsString)
                        .put(BODY_AS_BYTE_ARRAY, bodyAsByteArray)
                        .put(BODY_AS_MULTIPART, bodyAsMultipart);
    }


    public static final class Builder {

        private Struct struct;
        private String url;
        private String method;
        private String bodyType;
        private String stringBody;
        private byte[] byteArrayBody;
        private List<byte[]> multipartBody;
        private Map<String, List<String>> headers;

        private Builder() {
        }

        public static Builder anHttpRequest() {
            return new Builder();
        }

        public Builder withStruct(Struct struct) {
            this.struct = struct;
            //request
            this.url = struct.getString(URL);
            this.headers = struct.getMap(HEADERS);

            this.method = struct.getString(METHOD);
            this.bodyType = struct.getString(BODY_TYPE);
            this.stringBody = struct.getString(BODY_AS_STRING);
            this.byteArrayBody = Base64.getDecoder().decode(struct.getString(BODY_AS_BYTE_ARRAY));
            this.multipartBody = struct.getArray(BODY_AS_MULTIPART);

            return this;
        }


        public HttpRequest build() {


            HttpRequest httpRequest = new HttpRequest(
                    url,
                    method,
                    bodyType,
                    stringBody,
                    byteArrayBody,
                    multipartBody
            );

            httpRequest.setHeaders(headers);

            return httpRequest;
        }
    }


    private enum BodyType {
        STRING,
        BYTE_ARRAY,
        MULTIPART

    }
}
