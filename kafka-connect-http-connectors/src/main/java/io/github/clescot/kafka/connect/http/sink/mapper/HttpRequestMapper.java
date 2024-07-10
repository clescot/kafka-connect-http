package io.github.clescot.kafka.connect.http.sink.mapper;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.regex.Pattern;

public interface HttpRequestMapper {

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

    void setSplitLimit(int splitLimit);

    void setSplitPattern(String splitPattern);

    int getSplitLimit();

    Pattern getSplitPattern();
}
