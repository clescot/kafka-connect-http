package io.github.clescot.kafka.connect.http.core;

import java.util.Map;

public interface Request {

    String VU_ID = "VUID";

    Map<String, String> getAttributes();
}
