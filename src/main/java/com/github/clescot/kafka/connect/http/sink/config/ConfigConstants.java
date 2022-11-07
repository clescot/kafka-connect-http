package com.github.clescot.kafka.connect.http.sink.config;

public class ConfigConstants {



    protected ConfigConstants() {
        //Class with only constants
    }


    public static final String SUCCESS_TOPIC = "success.topic";
    public static final String SUCCESS_TOPIC_DOC = "Topic to receive successful http request/responses";
    public static final String ERRORS_TOPIC = "errors.topic";
    public static final String ERRORS_TOPIC_DOC = "Topic to receive errors from http request/responses";

    public static final String QUEUE_NAME = "queue.name";
    public static final String QUEUE_NAME_DOC = "queue name in the in memory map.";

    public static final String STATIC_REQUEST_HEADERS = "static.request.headers";
    public static final String STATIC_REQUEST_HEADERS_DOC = "list of static parameters names which will be added to all http requests. these parameter names need to be added with their values as parameters in complement of this list";


}
