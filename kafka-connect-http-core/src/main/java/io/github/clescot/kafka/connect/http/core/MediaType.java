package io.github.clescot.kafka.connect.http.core;

public class MediaType {

    private MediaType(){}
    public static final String KEY = "Content-Type";
    public static final String APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    public static final String MULTIPART_FORM_DATA = "multipart/form-data";
}