package io.github.clescot.core.http;

import java.util.Map;

public interface Request {

    String VU_ID = "vu_id";
    String DEFAULT_VU_ID = "default";

    Map<String, Object> getAttributes();

    long getLength();
}
