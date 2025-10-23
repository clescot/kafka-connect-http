package io.github.clescot.kafka.connect.http.core;

import java.util.Map;

public interface Response {

    Map<String, Object> getAttributes();
}
