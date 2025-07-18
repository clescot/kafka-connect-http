package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.HttpConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SseConfiguration<C extends HttpClient<R, S>, R, S> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SseConfiguration.class);

    private final Configuration<C, R, S> configuration;

    public <C extends HttpClient<R, S>,R,S> SseConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }


}
