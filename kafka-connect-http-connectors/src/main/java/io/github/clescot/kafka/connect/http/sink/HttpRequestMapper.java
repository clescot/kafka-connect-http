package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.connect.sink.SinkRecord;

public interface HttpRequestMapper {

    /**
     * does this instance can be used to map this sinkRecord to an HttpRequest.
     * @param sinkRecord
     * @return
     */
    boolean matches(SinkRecord sinkRecord);

    /**
     *  map this sinkRecord to an HttpRequest.
     * @param sinkRecord
     * @return
     */
    HttpRequest map(SinkRecord sinkRecord);
}
