package io.github.clescot.kafka.connect.http.core;

import java.util.Map;

public interface Exchange<R extends Request,S extends Response> {

    Map<String, String> getAttributes();

    R getRequest();
    S getResponse();

}
