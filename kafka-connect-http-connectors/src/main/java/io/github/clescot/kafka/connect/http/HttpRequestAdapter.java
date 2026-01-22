package io.github.clescot.kafka.connect.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.github.clescot.core.http.BodyType;
import io.github.clescot.core.http.HttpPart;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.MediaType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.github.clescot.core.http.HttpRequest.*;

public class HttpRequestAdapter {

    public static final int HTTP_EXCHANGE_VERSION = 2;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpRequestAdapter.class);
    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpPart.class.getName())
            .version(VERSION)
            .field(URL_FIELD, Schema.STRING_SCHEMA)
            .field(HEADERS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).schema()).build())
            .field(METHOD_FIELD, Schema.STRING_SCHEMA)
            .field(BODY_TYPE_FIELD, Schema.STRING_SCHEMA)
            .field(BODY_AS_BYTE_ARRAY_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(BODY_AS_FORM_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .field(BODY_AS_STRING_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(PARTS_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, HttpPartAdapter.SCHEMA).optional().schema())
            .field(ATTRIBUTES_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().schema())
            .schema();

    private HttpRequest httpRequest;

    private HttpRequestAdapter(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public HttpRequestAdapter(Struct requestAsstruct) {
        String url = requestAsstruct.getString(URL_FIELD);
        Preconditions.checkNotNull(url, "'url' is required");

        Map<String, List<String>> headers = requestAsstruct.getMap(HEADERS_FIELD);
        if (headers == null || headers.isEmpty()) {
            headers = Maps.newHashMap();
        }

        HttpRequest.Method method = HttpRequest.Method.valueOf(requestAsstruct.getString(METHOD_FIELD).toUpperCase());
        Preconditions.checkNotNull(method, "'method' is required");

        BodyType bodyType = BodyType.valueOf(Optional.ofNullable(requestAsstruct.getString(BODY_TYPE_FIELD)).orElse(BodyType.STRING.name()));

        String bodyAsByteArrayField = requestAsstruct.getString(BODY_AS_BYTE_ARRAY_FIELD);
        byte[] bodyAsByteArray = bodyAsByteArrayField!=null?bodyAsByteArrayField.getBytes(StandardCharsets.UTF_8):null;
        String bodyAsString = requestAsstruct.getString(BODY_AS_STRING_FIELD);
        Map bodyAsForm = requestAsstruct.getMap(BODY_AS_FORM_FIELD);

        Map<String, Struct> structs = requestAsstruct.getMap(PARTS_FIELD);
        Map<String,HttpPart> parts = Maps.newHashMap();
        if (structs != null) {
            //this is a multipart request

            for (Map.Entry<String, Struct> entry : structs.entrySet()) {
                HttpPart httpPart = HttpPartAdapter.from(entry.getValue()).toHttpPart();
                parts.put(entry.getKey(), httpPart);
                if (!headersFromPartAreValid(httpPart)) {
                    LOGGER.warn("this is a multipart request. headers from part are not valid : there is at least one header that is not 'Content-Disposition', 'Content-Type' or 'Content-Transfer-Encoding'. clearing headers from this part");
                    httpPart.getHeaders().clear();
                }
            }
        }
        this.httpRequest = new HttpRequest(url,method,headers,bodyType,parts);
        if(bodyAsByteArray!=null &&bodyAsByteArray.length>0){
            this.httpRequest.setBodyAsByteArray(bodyAsByteArray);
        }
        if(bodyAsString!=null){
            this.httpRequest.setBodyAsString(bodyAsString);
        }
        if(bodyAsForm!=null && !bodyAsForm.isEmpty()){
            this.httpRequest.setBodyAsForm(bodyAsForm);
        }

    }

    private boolean headersFromPartAreValid(HttpPart httpPart) {
        Map<String, List<String>> headersFromPart = httpPart.getHeaders();
        if (headersFromPart != null && !headersFromPart.isEmpty()) {
            return headersFromPart.keySet().stream()
                    .filter(key -> !key.equalsIgnoreCase("Content-Disposition"))
                    .filter(key -> !key.equalsIgnoreCase(MediaType.KEY))
                    .filter(key -> !key.equalsIgnoreCase("Content-Transfer-Encoding"))
                    .findAny().isEmpty();

        }
        return true;
    }

    public static HttpRequestAdapter from(HttpRequest httpRequest){
        return new HttpRequestAdapter(httpRequest);
    }
    public static HttpRequestAdapter from(Struct struct){
        return new HttpRequestAdapter(struct);
    }

    public Struct toStruct() {
        return new Struct(SCHEMA)
                .put(URL_FIELD, httpRequest.getUrl())
                .put(ATTRIBUTES_FIELD, httpRequest.getAttributes())
                .put(HEADERS_FIELD, httpRequest.getHeaders())
                .put(METHOD_FIELD, httpRequest.getMethod().name())
                .put(BODY_TYPE_FIELD, httpRequest.getBodyType().name())
                .put(BODY_AS_BYTE_ARRAY_FIELD, httpRequest.getBodyAsByteArray()!=null?new String(httpRequest.getBodyAsByteArray(), StandardCharsets.UTF_8):null)
                .put(BODY_AS_FORM_FIELD, httpRequest.getBodyAsForm())
                .put(BODY_AS_STRING_FIELD, httpRequest.getBodyAsString())
                .put(PARTS_FIELD,
                        httpRequest.getParts().entrySet().stream()
                                .collect(
                                        Collectors.toMap(Map.Entry::getKey,
                                                entry -> HttpPartAdapter.from(entry.getValue()).toStruct())
                                )
                )
                ;
    }

    public HttpRequest toHttpRequest(){
        return this.httpRequest;
    }

}
