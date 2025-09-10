package io.github.clescot.kafka.connect.http.core;

import java.util.Map;

public interface Exchange {

    Map<String, String> getAttributes();
}
