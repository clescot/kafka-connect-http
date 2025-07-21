package io.github.clescot.kafka.connect.http.sink.mapper;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Interface to map a {@link SinkRecord} to an {@link HttpRequest}.
 * Implementations should provide the logic to determine if they can handle a specific record and how to map it.
 */
public interface HttpRequestMapper {


    String getId();
    /**
     * does this instance can be used to map this sinkRecord to an HttpRequest.
     * @param sinkRecord message to map
     * @return true or false
     */
    boolean matches(ConnectRecord sinkRecord);

    /**
     *  map this sinkRecord to an HttpRequest.
     * @param sinkRecord message to map
     * @return built {{@link HttpRequest}
     */
    HttpRequest map(ConnectRecord sinkRecord);

}
