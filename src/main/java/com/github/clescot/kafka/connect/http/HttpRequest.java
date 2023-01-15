package com.github.clescot.kafka.connect.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpRequest.SCHEMA_AS_STRING,
        refs = {})
public class HttpRequest {

    public static final String test="";
    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";

    //only one 'body' field must be set
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_FORM = "bodyAsForm";
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
    private String bodyAsString = "";
    @JsonProperty
    private Map<String,String> bodyAsForm = Maps.newHashMap();
    @JsonProperty
    private String bodyAsByteArray = "";
    @JsonProperty
    private List<String> bodyAsMultipart = Lists.newArrayList();
    @JsonProperty
    private BodyType bodyType;

    public static final String SCHEMA_AS_STRING = "{\n" +
            "  \"$id\": \"https://github.com/clescot/kafka-connect-http-sink/schemas/http-request\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema\",\n" +
            "  \"title\": \"Http Request schema to drive HTTP Sink Connector\",\n" +
            "  \"description\": \"Http Request schema to drive HTTP Sink Connector. It supports 3 modes : classical body as string (bodyPart set to 'STRING'), a Map<String,String> mode to fill HTML form, a byte Array mode to transmit binary data((bodyPart set to 'BYTE_ARRAY'), and a multipart mode ((bodyPart set to 'MULTIPART')\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"additionalProperties\": false,\n" +
            "  \"required\": [\"url\",\"method\",\"bodyType\"],\n" +
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
            "    \"bodyType\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"enum\": [\n" +
            "        \"STRING\",\n" +
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
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA))
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_MULTIPART, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA));

    public HttpRequest(String url,
                       String method,
                       String bodyType) {
        Preconditions.checkNotNull(url, "url is required");
        Preconditions.checkNotNull(bodyType, "bodyType is required");
        this.url = url;
        this.method = method;
        this.bodyType = BodyType.valueOf(bodyType);
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
        return this.bodyAsByteArray != null ? Base64.getDecoder().decode(bodyAsByteArray) : new byte[0];
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

    public void setBodyAsString(String bodyAsString) {
        this.bodyAsString = bodyAsString;
    }

    public void setBodyAsForm(Map<String, String> bodyAsForm) {
        this.bodyAsForm = bodyAsForm;
    }

    public void setBodyAsByteArray( byte[] bodyAsByteArray) {
        this.bodyAsByteArray = Base64.getEncoder().encodeToString(bodyAsByteArray);
    }

    public void setBodyAsMultipart(List<byte[]> bodyAsMultipart) {
        if(bodyAsMultipart!=null) {
            this.bodyAsMultipart = convertMultipart(bodyAsMultipart);
        }
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpRequest that = (HttpRequest) o;
        return url.equals(that.url) && Objects.equals(headers, that.headers) && method.equals(that.method) && Objects.equals(bodyAsString, that.bodyAsString) && Objects.equals(bodyAsForm, that.bodyAsForm) && Objects.equals(bodyAsByteArray, that.bodyAsByteArray) && Objects.equals(bodyAsMultipart, that.bodyAsMultipart) && bodyType == that.bodyType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, headers, method, bodyAsString,bodyAsForm, bodyAsByteArray, bodyAsMultipart, bodyType);
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "url='" + url + '\'' +
                ", headers=" + headers +
                ", method='" + method + '\'' +
                ", bodyAsString='" + bodyAsString + '\'' +
                ", bodyAsForm='" + bodyAsForm + '\'' +
                ", bodyAsByteArray='" + bodyAsByteArray + '\'' +
                ", bodyAsMultipart=" + bodyAsMultipart +
                ", bodyType=" + bodyType +
                '}';
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(URL, url)
                .put(HEADERS, headers)
                .put(METHOD, method)
                .put(BODY_TYPE, bodyType.name())
                .put(BODY_AS_STRING, bodyAsString)
                .put(BODY_AS_FORM, bodyAsForm)
                .put(BODY_AS_BYTE_ARRAY, bodyAsByteArray)
                .put(BODY_AS_MULTIPART, bodyAsMultipart);
    }


    public static final class Builder {

        private Struct struct;
        private String url;
        private String method;
        private String bodyType;
        private String stringBody;
        private Map<String,String> formBody = Maps.newHashMap();
        private byte[] byteArrayBody;
        private List<byte[]> multipartBody = Lists.newArrayList();
        private Map<String, List<String>> headers = Maps.newHashMap();

        private Builder() {
        }

        public static Builder anHttpRequest() {
            return new Builder();
        }

        public Builder withStruct(Struct struct) {
            this.struct = struct;
            //request

            this.url = struct.getString(URL);
            Preconditions.checkNotNull(url, "'url' is required");

            Map<String, List<String>> headers = struct.getMap(HEADERS);
            if(headers!=null&&!headers.isEmpty()) {
                this.headers = headers;
            }

            this.method = struct.getString(METHOD);
            Preconditions.checkNotNull(method, "'method' is required");

            this.bodyType = struct.getString(BODY_TYPE);
            Preconditions.checkNotNull(method, "'bodyType' is required");
            LOGGER.debug("withStruct: stringBody:'{}'",struct.getString(BODY_AS_STRING));
            this.stringBody = struct.getString(BODY_AS_STRING);
            this.formBody = struct.getMap(BODY_AS_FORM);
            this.byteArrayBody = Base64.getDecoder().decode(Optional.ofNullable(struct.getString(BODY_AS_BYTE_ARRAY)).orElse(""));
            List<byte[]> array = struct.getArray(BODY_AS_MULTIPART);
            if(array!=null) {
                this.multipartBody = array;
            }

            return this;
        }


        public HttpRequest build() {


            HttpRequest httpRequest = new HttpRequest(
                    url,
                    method,
                    bodyType
            );

            httpRequest.setHeaders(headers);
            LOGGER.debug("bodyType='{}'",bodyType.toString());
            if(BodyType.STRING.name().equals(bodyType)){
                httpRequest.setBodyAsString(this.stringBody);
                LOGGER.debug("stringBody='{}'",stringBody);
                LOGGER.debug("httpRequest='{}'",httpRequest);
            }else if(BodyType.BYTE_ARRAY.name().equals(bodyType)){
                LOGGER.debug("byteArrayBody='{}'",byteArrayBody);
                httpRequest.setBodyAsByteArray(this.byteArrayBody);
            }else if(BodyType.FORM.name().equals(this.bodyType)){
                LOGGER.debug("formBody='{}'",formBody);
                httpRequest.setBodyAsForm(this.formBody);
            }else if(BodyType.MULTIPART.name().equals(this.bodyType)){
                LOGGER.debug("multipartBody='{}'",multipartBody);
                httpRequest.setBodyAsMultipart(multipartBody);
            }else{
                LOGGER.error("unknown BodyType: '",bodyType);
                throw new IllegalArgumentException("unknown BodyType: '"+bodyType+"'");
            }
            return httpRequest;
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


}
