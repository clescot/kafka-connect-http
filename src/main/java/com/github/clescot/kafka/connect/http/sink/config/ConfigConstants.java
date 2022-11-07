package com.github.clescot.kafka.connect.http.sink.config;

public class ConfigConstants {

    protected ConfigConstants() {
        //Class with only constants
    }

    public static final String SUCCESS_TOPIC = "success.topic";
    public static final String SUCCESS_TOPIC_DOC = "Topic to receive successful http request/responses";
    public static final String ERRORS_TOPIC = "errors.topic";
    public static final String ERRORS_TOPIC_DOC = "Topic to receive errors from http request/responses";



}
