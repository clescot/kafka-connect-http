package io.github.clescot.kafka.connect.http.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
@io.confluent.kafka.schemaregistry.annotations.Schema(value = HttpResponse.SCHEMA_AS_STRING,
        refs = {})
public class HttpResponse implements Serializable {
    private static final Integer VERSION = 2;

    public static final String STATUS_CODE = "statusCode";
    public static final String STATUS_MESSAGE = "statusMessage";
    public static final String PROTOCOL = "protocol";
    public static final String HEADERS = "headers";
    public static final String BODY_AS_STRING = "bodyAsString";

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE,Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE,Schema.STRING_SCHEMA)
            .field(PROTOCOL,Schema.OPTIONAL_STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY_AS_STRING,Schema.OPTIONAL_STRING_SCHEMA);
    private static final long serialVersionUID = 1L;
    public static final String SCHEMA_ID = HttpExchange.BASE_SCHEMA_ID+"http-response.json";
    public static final String SCHEMA_AS_STRING = "{\n" +
            "  \"$id\": \"https://raw.githubusercontent.com/clescot/kafka-connect-http/master/kafka-connect-http-core/src/main/resources/schemas/json/versions/2/http-response.json\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft/2019-09/schema#\",\n" +
            "  \"title\": \"Http Response\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"additionalProperties\": false,\n" +
            "  \"properties\": {\n" +
            "    \"statusCode\":{\n" +
            "      \"type\": \"integer\"\n" +
            "    },\n" +
            "    \"statusMessage\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"protocol\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"headers\":  {\n" +
            "      \"type\": \"object\",\n" +
            "      \"additionalProperties\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"string\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"bodyAsString\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"bodyAsForm\":\n" +
            "    {\n" +
            "      \"type\": \"object\",\n" +
            "      \"connect.type\": \"map\",\n" +
            "      \"additionalProperties\" : { \"type\": \"string\" }\n" +
            "    },\n" +
            "    \"bodyAsByteArray\":  {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"bodyAsMultipart\": {\n" +
            "      \"type\": \"array\",\n" +
            "      \"items\": {\n" +
            "        \"type\": \"string\"\n" +
            "        }" +
            "       },\n" +
            "    \"bodyType\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"enum\": [\n" +
            "        \"STRING\",\n" +
            "        \"FORM\",\n" +
            "        \"BYTE_ARRAY\",\n" +
            "        \"MULTIPART\"\n" +
            "      ]\n" +
            "     }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"statusCode\",\n" +
            "    \"statusMessage\"\n" +
            "  ]\n" +
            "}";

    @JsonProperty(required = true)
    private Integer statusCode;
    @JsonProperty(required = true)
    private String statusMessage;
    private String bodyAsString ="";
    private String protocol="";

    private Map<String, List<String>> headers = Maps.newHashMap();

    /**
     * only for json deserialization
     */
    protected HttpResponse() {
    }

    public HttpResponse(Integer statusCode, String statusMessage) {
        Preconditions.checkArgument(statusCode>0,"status code must be a positive integer");
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getBodyAsString() {
        return bodyAsString;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = headers;
    }

    public void setBodyAsString(String bodyAsString) {
        this.bodyAsString = bodyAsString;
    }


    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    protected void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    protected void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HttpResponse that = (HttpResponse) o;
        return statusCode.equals(that.statusCode) && statusMessage.equals(that.statusMessage) && protocol.equals(that.protocol)&& bodyAsString.equals(that.bodyAsString) && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statusCode, statusMessage, bodyAsString, headers);
    }

    @Override
    public String toString() {
        return "HttpResponse{" +
                "statusCode=" + statusCode +
                ", statusMessage='" + statusMessage + '\'' +
                ", protocol='" + protocol + '\'' +
                ", responseBody='" + bodyAsString + '\'' +
                ", responseHeaders=" + headers +
                '}';
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(STATUS_CODE,this.getStatusCode().longValue())
                .put(STATUS_MESSAGE,this.getStatusMessage())
                .put(HEADERS,this.getHeaders())
                .put(BODY_AS_STRING,this.getBodyAsString());
    }
}
