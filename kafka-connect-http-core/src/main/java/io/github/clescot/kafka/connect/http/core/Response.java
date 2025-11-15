package io.github.clescot.kafka.connect.http.core;

import java.util.Map;

public interface Response {

    boolean isSuccess();

    Map<String, Object> getAttributes();
}
