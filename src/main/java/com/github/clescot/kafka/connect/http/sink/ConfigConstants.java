package com.github.clescot.kafka.connect.http.sink;

public class ConfigConstants {

    protected ConfigConstants() {
        //Class with only constants
    }

    //common parameters
    public static final String QUEUE_NAME = "queue.name";
    public static final String QUEUE_NAME_DOC = "queue name in the in memory map.";


    //sink parameters
    public static final String STATIC_REQUEST_HEADER_NAMES = "static.request.header.names";
    public static final String STATIC_REQUEST_HEADER_NAMES_DOC = "list of static parameters names which will be added to all http requests. these parameter names need to be added with their values as parameters in complement of this list";
    public static final String PUBLISH_TO_IN_MEMORY_QUEUE = "publish.to.in.memory.queue";
    public static final String PUBLISH_TO_IN_MEMORY_QUEUE_DOC = "when set to false, ignore HTTP responses, i.e does not publish responses in the in memory queue. No Source Connector is needed when set to false. When set to true, a Source Connector is needed to consume published Acknowledgment in this in memory queue.";

    //source parameters
    public static final String SUCCESS_TOPIC = "success.topic";
    public static final String SUCCESS_TOPIC_DOC = "Topic to receive successful http request/responses";
    public static final String ERROR_TOPIC = "error.topic";
    public static final String ERROR_TOPIC_DOC = "Topic to receive errors from http request/responses";


}
