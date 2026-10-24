package io.github.clescot.core.http;

import java.util.Map;

public interface Response {

    boolean isSuccess();

    Map<String, Object> getAttributes();
}
