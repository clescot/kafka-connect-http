package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import com.launchdarkly.eventsource.*;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;

import java.net.URI;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;


public class SseConfiguration implements Configuration<OkHttpClient, HttpRequest> {
    private final String configurationId;
    private final HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration;
    private final Map<String, String> settings;
    private final URI uri;
    private final String topic;
    private boolean connected = false;
    private boolean started = false;
    private SseBackgroundEventHandler backgroundEventHandler;
    private BackgroundEventSource backgroundEventSource;
    private Queue<SseEvent> queue;
    private HttpConnectStrategy connectStrategy;
    private DefaultRetryDelayStrategy retryDelayStrategy;
    private ErrorStrategy errorStrategy;

    public SseConfiguration(String configurationId,
                            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration,
                            Map<String, String> settings
    ) {
        Preconditions.checkNotNull(configurationId, "configurationId must not be null.");
        Preconditions.checkArgument(!configurationId.isEmpty(), "configurationId must not be empty.");
        Preconditions.checkNotNull(httpClientConfiguration, "httpClientConfiguration must not be null.");
        this.configurationId = configurationId;
        this.httpClientConfiguration = httpClientConfiguration;
        this.settings = settings;
        Preconditions.checkNotNull(settings, "settings must not be null or empty.");
        Preconditions.checkArgument(!settings.isEmpty(), "settings must not be null or empty.");
        String url = settings.get(SseConfigDefinition.URL);
        Preconditions.checkNotNull(url, "'url' must not be null or empty.");
        this.uri = URI.create(url);
        this.topic = settings.get(SseConfigDefinition.TOPIC);
        Preconditions.checkNotNull(topic, "'topic' must not be null or empty.");
    }

    public static SseConfiguration buildSseConfiguration(String configurationId, Map<String, String> mySettings) {
        HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>(
                configurationId,
                new OkHttpClientFactory(),
                mySettings,
                null,
                new CompositeMeterRegistry());
        return new SseConfiguration(configurationId, httpClientConfiguration, mySettings);
    }

    public BackgroundEventSource connect(Queue<SseEvent> queue) {
        Preconditions.checkNotNull(queue, "queue must not be null.");
        this.queue = queue;


        //retry strategy
        this.retryDelayStrategy = RetryDelayStrategy.defaultStrategy();
        if (settings.containsKey(SseConfigDefinition.RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS)) {
            long maxDelayMillis = Long.parseLong(settings.getOrDefault(SseConfigDefinition.RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS, "30000"));
            float backoffMultiplier = Float.parseFloat(settings.getOrDefault(SseConfigDefinition.RETRY_DELAY_STRATEGY_BACKOFF_MULTIPLIER, "2"));
            float jitterMultiplier = Float.parseFloat(settings.getOrDefault(SseConfigDefinition.RETRY_DELAY_STRATEGY_JITTER_MULTIPLIER, "0.5"));
            retryDelayStrategy = retryDelayStrategy
                    .maxDelay(maxDelayMillis, TimeUnit.MILLISECONDS)
                    .backoffMultiplier(backoffMultiplier)
                    .jitterMultiplier(jitterMultiplier);
        }

        //error strategy
        this.errorStrategy = ErrorStrategy.alwaysThrow();
        if (settings.containsKey(SseConfigDefinition.ERROR_STRATEGY)) {
            String errorStrategyAsString = settings.get(SseConfigDefinition.ERROR_STRATEGY);
            switch (errorStrategyAsString) {
                case SseConfigDefinition.ERROR_STRATEGY_ALWAYS_CONTINUE:
                    errorStrategy = ErrorStrategy.alwaysContinue();
                    break;
                case SseConfigDefinition.ERROR_STRATEGY_ALWAYS_THROW:
                    errorStrategy = ErrorStrategy.alwaysThrow();
                    break;
                case SseConfigDefinition.ERROR_STRATEGY_CONTINUE_WITH_MAX_ATTEMPTS:
                    int maxAttempts = Integer.parseInt(settings.getOrDefault(SseConfigDefinition.ERROR_STRATEGY_MAX_ATTEMPTS, "3"));
                    errorStrategy = ErrorStrategy.continueWithMaxAttempts(maxAttempts);
                    break;
                case SseConfigDefinition.ERROR_STRATEGY_CONTINUE_WITH_TIME_LIMIT:
                    long timeLimitCountInMillis = Long.parseLong(settings.getOrDefault(SseConfigDefinition.ERROR_STRATEGY_TIME_LIMIT_COUNT_IN_MILLIS, "60000"));
                    errorStrategy = ErrorStrategy.continueWithTimeLimit(timeLimitCountInMillis, TimeUnit.MILLISECONDS);
                    break;
                default:
                    errorStrategy = ErrorStrategy.alwaysThrow();
            }
        }
        this.backgroundEventHandler = new SseBackgroundEventHandler(queue, uri);
        this.connectStrategy = ConnectStrategy.http(uri)
                .httpClient(this.httpClientConfiguration.getClient().getInternalClient())
                .requestTransformer(input -> {
                    HttpRequest httpRequest = this.httpClientConfiguration.getClient().buildRequest(input);
                    return this.httpClientConfiguration.getClient().buildNativeRequest(this.httpClientConfiguration.getEnrichRequestFunction().apply(httpRequest));
                });
        this.backgroundEventSource = new BackgroundEventSource.Builder(backgroundEventHandler,
                new EventSource.Builder(connectStrategy
                )
                .streamEventData(false)
                .retryDelayStrategy(retryDelayStrategy)
                .errorStrategy(errorStrategy)
        ).build();
        connected = true;
        return backgroundEventSource;
    }

    public void start() {
        Preconditions.checkState(connected, "SSE client is not connected. Call connect() first.");
        if (backgroundEventSource != null) {
            backgroundEventSource.start();
        }
        started = true;
    }

    public void stop() {
        connected = false;
        started = false;
        if (backgroundEventSource != null) {
            backgroundEventSource.close();
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isStarted() {
        return started;
    }

    public SseBackgroundEventHandler getBackgroundEventHandler() {
        return backgroundEventHandler;
    }

    @Override
    public boolean matches(HttpRequest request) {
        return false;
    }

    @Override
    public OkHttpClient getClient() {
        return httpClientConfiguration.getClient();
    }

    public Queue<SseEvent> getQueue() {
        return queue;
    }

    public BackgroundEventSource getBackgroundEventSource() {
        return backgroundEventSource;
    }

    public String getConfigurationId() {
        return configurationId;
    }

    public String getTopic() {
        return topic;
    }

    public URI getUri() {
        return uri;
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    public HttpConnectStrategy getConnectStrategy() {
        return connectStrategy;
    }

    public DefaultRetryDelayStrategy getRetryDelayStrategy() {
        return retryDelayStrategy;
    }

    public ErrorStrategy getErrorStrategy() {
        return errorStrategy;
    }
}
