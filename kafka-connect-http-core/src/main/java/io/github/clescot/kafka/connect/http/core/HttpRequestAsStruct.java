package io.github.clescot.kafka.connect.http.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HttpRequestAsStruct {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRequestAsStruct.class);
    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";

    //only one 'body' field must be set
    public static final String BODY_TYPE = "bodyType";
    public static final String BODY_AS_STRING = "bodyAsString";
    public static final String BODY_AS_FORM = "bodyAsForm";
    public static final String BODY_AS_BYTE_ARRAY = "bodyAsByteArray";
    public static final String BODY_AS_MULTIPART = "bodyAsMultipart";
    public static final int VERSION = 2;

    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpRequest.class.getName())
            .version(VERSION)
            //request
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).build())
            .field(URL, Schema.STRING_SCHEMA)
            .field(METHOD, Schema.STRING_SCHEMA)
            .field(BODY_TYPE, Schema.STRING_SCHEMA)
            .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM, SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).schema())
            .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_MULTIPART, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).schema())
            .schema();

    private HttpRequest httpRequest;

    public HttpRequestAsStruct(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(URL, httpRequest.getUrl())
                .put(HEADERS, httpRequest.getHeaders())
                .put(METHOD, httpRequest.getMethod().name())
                .put(BODY_TYPE, httpRequest.getBodyType().name())
                .put(BODY_AS_STRING,httpRequest.getBodyAsString())
                .put(BODY_AS_FORM, httpRequest.getBodyAsForm())
                .put(BODY_AS_BYTE_ARRAY, Base64.getEncoder().encodeToString(httpRequest.getBodyAsByteArray()))
                .put(BODY_AS_MULTIPART, httpRequest.getBodyAsMultipart());
    }

    public static final class Builder {

        private Struct struct;
        private String url;
        private HttpRequest.Method method;
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

            this.method = HttpRequest.Method.valueOf(struct.getString(METHOD).toUpperCase());
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
            LOGGER.debug("bodyType='{}'", bodyType);
            if(HttpRequest.BodyType.STRING.name().equals(bodyType)){
                httpRequest.setBodyAsString(this.stringBody);
                LOGGER.debug("stringBody='{}'",stringBody);
                LOGGER.debug("httpRequest='{}'",httpRequest);
            }else if(HttpRequest.BodyType.BYTE_ARRAY.name().equals(bodyType)){
                LOGGER.debug("byteArrayBody='{}'",byteArrayBody);
                httpRequest.setBodyAsByteArray(this.byteArrayBody);
            }else if(HttpRequest.BodyType.FORM.name().equals(this.bodyType)){
                LOGGER.debug("formBody='{}'",formBody);
                httpRequest.setBodyAsForm(this.formBody);
            }else if(HttpRequest.BodyType.MULTIPART.name().equals(this.bodyType)){
                LOGGER.debug("multipartBody='{}'",multipartBody);
                httpRequest.setBodyAsMultipart(multipartBody);
            }else{
                LOGGER.error("unknown BodyType: '{}'",bodyType);
                throw new IllegalArgumentException("unknown BodyType: '"+bodyType+"'");
            }
            return httpRequest;
        }
    }
}
