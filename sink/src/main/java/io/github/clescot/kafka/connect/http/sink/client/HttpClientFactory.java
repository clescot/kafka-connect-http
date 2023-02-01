package io.github.clescot.kafka.connect.http.sink.client;

import java.util.Map;

public interface HttpClientFactory {






    HttpClient build(Map<String, String> config);
}
