package io.github.clescot.kafka.connect.http;

import com.google.common.collect.Maps;
import io.github.clescot.core.http.HttpPart;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public class HttpPartAdapter {

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
    private HttpPart httpPart;

    //for serialization
    private HttpPartAdapter(Struct struct) {
        Map headers = struct.getMap(HEADERS)!=null?struct.getMap(HEADERS): Maps.newHashMap();
        HttpPart.BodyType bodyType = HttpPart.BodyType.valueOf(struct.getString(BODY_TYPE));
        String contentAsByteArray = struct.getString(BODY_AS_BYTE_ARRAY);
        if(contentAsByteArray!=null && !contentAsByteArray.isEmpty()){
            this.httpPart = new HttpPart(headers,contentAsByteArray);
        }
        String contentAsString = struct.getString(BODY_AS_STRING);
        if(contentAsString!=null && !contentAsString.isEmpty()){
            this.httpPart = new HttpPart(headers,contentAsString);
        }
        //this.contentAsFormEntry = struct.getMap(BODY_AS_FORM_DATA);

    }

    private HttpPartAdapter(HttpPart httpPart){
        this.httpPart = httpPart;
    }


    public static HttpPartAdapter from(HttpPart httpPart){
        return new HttpPartAdapter(httpPart);
    }
    public static HttpPartAdapter from(Struct struct){
        return new HttpPartAdapter(struct);
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA);
        struct.put(HEADERS, httpPart.getHeaders());
        struct.put(BODY_TYPE, httpPart.getBodyType().name());
        struct.put(BODY_AS_STRING, httpPart.getContentAsString());
        struct.put(BODY_AS_FORM_DATA, httpPart.getContentAsFormEntry());
        struct.put(BODY_AS_BYTE_ARRAY, new String(httpPart.getContentAsByteArray()));
        struct.put(FILE_URI, httpPart.getFileUri());
        return struct;
    }

    public HttpPart toHttpPart(){
        return this.httpPart;
    }

}
