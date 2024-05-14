package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.connect.sink.SinkRecord;

public interface HttpRequestMapper {

    boolean matches(SinkRecord sinkRecord);

    HttpRequest map(SinkRecord sinkRecord);
}
