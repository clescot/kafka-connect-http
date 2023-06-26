package io.github.clescot.kafka.connect.http.sink.config;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.core.HttpRequest;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AddStaticHeadersFunction implements Function<HttpRequest,HttpRequest> {

    private final Map<String, List<String>> staticHeaders;

    public AddStaticHeadersFunction(Map<String, List<String>> staticHeaders) {
        Preconditions.checkNotNull(staticHeaders, "staticHeaders map is null");
        this.staticHeaders = staticHeaders;
    }

    @Override
    public HttpRequest apply(HttpRequest httpRequest) {
        Preconditions.checkNotNull(httpRequest, "httpRequest is null");
        this.staticHeaders.forEach((key, value) -> httpRequest.getHeaders().put(key, value));
        return httpRequest;
    }

    public Map<String, List<String>> getStaticHeaders() {
        return staticHeaders;
    }
}
