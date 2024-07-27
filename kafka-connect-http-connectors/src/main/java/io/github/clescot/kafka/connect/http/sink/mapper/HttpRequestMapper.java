package io.github.clescot.kafka.connect.http.sink.mapper;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.connect.sink.SinkRecord;

public interface HttpRequestMapper {


    String getId();
    /**
     * does this instance can be used to map this sinkRecord to an HttpRequest.
     * @param sinkRecord message to map
     * @return true or false
     */
    boolean matches(SinkRecord sinkRecord);

    /**
     *  map this sinkRecord to an HttpRequest.
     * @param sinkRecord message to map
     * @return built {{@link HttpRequest}
     */
    HttpRequest map(SinkRecord sinkRecord);

}
