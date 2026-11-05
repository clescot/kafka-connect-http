package io.github.clescot.kafka.connect.sse.client.okhttp;

import static io.github.clescot.client.Constants.EXCHANGE_TEMPLATE;
import static io.github.clescot.client.Constants.STATIC_REQUEST_HEADER_NAMES;
import static io.github.clescot.client.http.HttpClientFactory.getRandom;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.launchdarkly.eventsource.*;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import io.github.clescot.client.Configuration;
import io.github.clescot.client.http.HttpClientFactory;
import io.github.clescot.client.http.okhttp.OkHttpClient;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.http.template.ExchangeTemplateManager;
import io.github.clescot.core.http.template.ExchangeTemplateProcessorFactory;
import io.github.clescot.core.sse.SseEvent;
import io.github.clescot.core.sse.SseExchange;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configuration for SSE client using OkHttp. */
public class SseConfiguration implements Configuration<OkHttpClient, HttpRequest> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SseConfiguration.class);

  /**
   * The SSE resume header: the only lever the connector has on the position a stream starts from.
   */
  static final String LAST_EVENT_ID_HEADER = "Last-Event-ID";

  private final String configurationId;
  private OkHttpClient httpClient;
  private final Map<String, String> settings;
  private final URI uri;
  private final String topic;
  private final String id;
  private final SseResumePolicy resumePolicy;
  private boolean askFromStart = false;
  private String fromStartEventId;
  private boolean connected = false;
  private boolean started = false;
  private SseBackgroundEventHandler backgroundEventHandler;
  private BackgroundEventSource backgroundEventSource;
  private Queue<SseEvent> queue;
  private HttpConnectStrategy connectStrategy;
  private DefaultRetryDelayStrategy retryDelayStrategy;
  private ErrorStrategy errorStrategy;
  private ExchangeTemplateManager templateManager;
  private String exchangeTemplate;

  /**
   * @param configurationId configuration id
   * @param httpClient HTTP Client
   * @param settings prefix config.configurationId.* should be removed
   */
  public SseConfiguration(
      String configurationId, OkHttpClient httpClient, Map<String, String> settings) {
    Preconditions.checkNotNull(configurationId, "configurationId must not be null.");
    Preconditions.checkArgument(!configurationId.isEmpty(), "configurationId must not be empty.");
    Preconditions.checkNotNull(httpClient, "httpClient must not be null.");
    this.configurationId = configurationId;
    this.httpClient = httpClient;
    this.settings = settings;
    Preconditions.checkNotNull(settings, "settings must not be null or empty.");
    Preconditions.checkArgument(!settings.isEmpty(), "settings must not be null or empty.");
    String url = settings.get(SseConfigDefinition.URL);
    this.id = configurationId;
    Preconditions.checkNotNull(url, "'url' must not be null or empty.");
    this.uri = URI.create(url);
    this.topic = settings.get(SseConfigDefinition.TOPIC);
    Preconditions.checkNotNull(topic, "'topic' must not be null or empty.");

    // Initialize template processing using shared utility
    this.exchangeTemplate = settings.getOrDefault(EXCHANGE_TEMPLATE, "");
    ExchangeTemplateProcessorFactory factory = new ExchangeTemplateProcessorFactory();
    this.templateManager = factory.createTemplateManager(settings);

    this.resumePolicy = SseResumePolicy.fromSettings(settings);
    this.fromStartEventId =
        settings.getOrDefault(
            SseConfigDefinition.RESUME_FROM_START_EVENT_ID,
            SseConfigDefinition.DEFAULT_RESUME_FROM_START_EVENT_ID);
    checkResumePolicyAgainstStaticHeaders();
  }

  public static SseConfiguration buildSseConfiguration(
      String configurationId,
      Map<String, String> mySettings,
      ExecutorService executorService,
      CompositeMeterRegistry meterRegistry,
      HttpClientFactory<OkHttpClient, Request, Response> httpClientFactory) {
    Random random = getRandom(mySettings);
    OkHttpClient httpClient =
        httpClientFactory.buildHttpClient(mySettings, executorService, meterRegistry, random);
    return new SseConfiguration(configurationId, httpClient, mySettings);
  }

  public BackgroundEventSource connect(Queue<SseEvent> queue) {
    return connect(queue, Optional.empty());
  }

  /**
   * Connects to the SSE server, seeding the resume cursor with the given committed event id: the
   * server is asked to replay from the last event Kafka Connect committed. An empty id means no
   * committed offset: the configuration's Resume Policy decides what is asked of the server, which
   * by default is nothing.
   *
   * @param queue queue receiving the SSE events
   * @param lastCommittedEventId highest event id committed by Kafka Connect, or empty
   * @return the background event source
   */
  public BackgroundEventSource connect(
      Queue<SseEvent> queue, Optional<String> lastCommittedEventId) {
    Preconditions.checkNotNull(queue, "queue must not be null.");
    Preconditions.checkNotNull(lastCommittedEventId, "lastCommittedEventId must not be null.");
    this.queue = queue;
    // the policy only speaks where nothing was committed: an offset is always the better answer
    this.askFromStart =
        resumePolicy == SseResumePolicy.FROM_START && lastCommittedEventId.isEmpty();

    // retry strategy
    this.retryDelayStrategy = RetryDelayStrategy.defaultStrategy();
    if (settings.containsKey(SseConfigDefinition.RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS)) {
      long maxDelayMillis =
          Long.parseLong(
              settings.getOrDefault(
                  SseConfigDefinition.RETRY_DELAY_STRATEGY_MAX_DELAY_MILLIS, "30000"));
      float backoffMultiplier =
          Float.parseFloat(
              settings.getOrDefault(
                  SseConfigDefinition.RETRY_DELAY_STRATEGY_BACKOFF_MULTIPLIER, "2"));
      float jitterMultiplier =
          Float.parseFloat(
              settings.getOrDefault(
                  SseConfigDefinition.RETRY_DELAY_STRATEGY_JITTER_MULTIPLIER, "0.5"));
      retryDelayStrategy =
          retryDelayStrategy
              .maxDelay(maxDelayMillis, TimeUnit.MILLISECONDS)
              .backoffMultiplier(backoffMultiplier)
              .jitterMultiplier(jitterMultiplier);
    }

    // error strategy
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
          int maxAttempts =
              Integer.parseInt(
                  settings.getOrDefault(SseConfigDefinition.ERROR_STRATEGY_MAX_ATTEMPTS, "3"));
          errorStrategy = ErrorStrategy.continueWithMaxAttempts(maxAttempts);
          break;
        case SseConfigDefinition.ERROR_STRATEGY_CONTINUE_WITH_TIME_LIMIT:
          long timeLimitCountInMillis =
              Long.parseLong(
                  settings.getOrDefault(
                      SseConfigDefinition.ERROR_STRATEGY_TIME_LIMIT_COUNT_IN_MILLIS, "60000"));
          errorStrategy =
              ErrorStrategy.continueWithTimeLimit(timeLimitCountInMillis, TimeUnit.MILLISECONDS);
          break;
        default:
          errorStrategy = ErrorStrategy.alwaysThrow();
      }
    }
    this.backgroundEventHandler = new SseBackgroundEventHandler(queue, uri);
    this.connectStrategy =
        ConnectStrategy.http(uri)
            .httpClient(this.httpClient.getInternalClient())
            .requestTransformer(
                input -> {
                  HttpRequest httpRequest = this.httpClient.buildRequest(input);
                  // before the operator's own enrichment, so an explicitly configured header wins
                  // over the connector's sentinel
                  applyResumePolicy(httpRequest);
                  return this.httpClient.buildNativeRequest(
                      this.httpClient.getEnrichRequestFunction().apply(httpRequest));
                });
    // the server may advertise an exhausted quota or a Retry-After delay: observe every attempt,
    // and never reconnect sooner than it asked. Only the delay changes; the resume cursor below
    // is untouched.
    SseRateLimitPacing pacing = SseRateLimitPacing.fromSettings(configurationId, uri, settings);
    this.backgroundEventSource =
        new BackgroundEventSource.Builder(
                backgroundEventHandler,
                new EventSource.Builder(pacing.connectStrategy(connectStrategy))
                    .streamEventData(false)
                    .lastEventId(lastCommittedEventId.orElse(null))
                    .retryDelayStrategy(pacing.retryDelayStrategy(retryDelayStrategy))
                    .errorStrategy(errorStrategy))
            .build();
    connected = true;
    return backgroundEventSource;
  }

  /**
   * Applies this configuration's Resume Policy to an outgoing request. When the policy asks for the
   * whole stream and the eventsource library put no cursor of its own on the request, the sentinel
   * event id is added: an absent {@code Last-Event-ID} is how the library says "nothing to resume
   * from", so its own cursor always wins and a mid-run reconnect resumes from it as before.
   *
   * @param httpRequest the request about to be sent, enriched in place
   */
  void applyResumePolicy(HttpRequest httpRequest) {
    Preconditions.checkNotNull(httpRequest, "httpRequest must not be null.");
    if (!askFromStart || hasHeader(httpRequest, LAST_EVENT_ID_HEADER)) {
      return;
    }
    httpRequest.getHeaders().put(LAST_EVENT_ID_HEADER, Lists.newArrayList(fromStartEventId));
  }

  private static boolean hasHeader(HttpRequest httpRequest, String name) {
    return httpRequest.getHeaders().keySet().stream()
        .anyMatch(headerName -> name.equalsIgnoreCase(headerName));
  }

  /**
   * Rejects a configuration that combines the {@code from-start} policy with a static {@code
   * Last-Event-ID} header, and warns about a static header on its own. A static header is re-sent
   * on every reconnect, so it never lets the stream resume from where it stopped.
   */
  private void checkResumePolicyAgainstStaticHeaders() {
    if (!hasStaticLastEventIdHeader()) {
      return;
    }
    if (resumePolicy == SseResumePolicy.FROM_START) {
      throw new IllegalArgumentException(
          "configuration "
              + configurationId
              + " sets a static Last-Event-ID header and resume.policy '"
              + SseResumePolicy.FROM_START.settingValue()
              + "': the static header is re-sent on every reconnect, so it would ask for the whole"
              + " stream forever instead of once. Configure one of the two.");
    }
    LOGGER.warn(
        "configuration {} sets a static Last-Event-ID header: it overrides the committed offset and"
            + " asks the server for that position on every reconnect, so the stream never resumes"
            + " from where it stopped",
        configurationId);
  }

  private boolean hasStaticLastEventIdHeader() {
    String headerNames = settings.get(STATIC_REQUEST_HEADER_NAMES);
    if (headerNames == null) {
      return false;
    }
    return Arrays.stream(headerNames.split(","))
        .anyMatch(name -> LAST_EVENT_ID_HEADER.equalsIgnoreCase(name.trim()));
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
  public String getId() {
    return id;
  }

  @Override
  public OkHttpClient getClient() {
    return httpClient;
  }

  @Override
  public void setClient(OkHttpClient client) {
    this.httpClient = client;
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

  /** Returns the configured Resume Policy of this configuration. */
  public SseResumePolicy getResumePolicy() {
    return resumePolicy;
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

  /**
   * Gets the template manager for processing SSE events.
   *
   * @return the ExchangeTemplateManager
   */
  public ExchangeTemplateManager getTemplateManager() {
    return templateManager;
  }

  /**
   * Sets the template manager for processing SSE events.
   *
   * @param templateManager the ExchangeTemplateManager to set
   */
  public void setTemplateManager(ExchangeTemplateManager templateManager) {
    this.templateManager = templateManager;
  }

  /**
   * Gets the exchange template for processing SSE events.
   *
   * @return the exchange template
   */
  public String getExchangeTemplate() {
    return exchangeTemplate;
  }

  /**
   * Sets the exchange template for processing SSE events.
   *
   * @param exchangeTemplate the template to set
   */
  public void setExchangeTemplate(String exchangeTemplate) {
    this.exchangeTemplate = exchangeTemplate;
  }

  /**
   * Processes an SSE event using the configured template.
   *
   * @param sseEvent the SSE event to process
   * @param httpRequest the HTTP request that initiated the SSE connection (can be null)
   * @return the processed SSE event, or the original event if no template is configured or
   *     processing fails
   */
  public SseEvent processEventWithTemplate(SseEvent sseEvent, HttpRequest httpRequest) {
    return processEventWithTemplate(sseEvent, httpRequest, Map.of());
  }

  /**
   * Processes an SSE event using the configured template with additional context.
   *
   * @param sseEvent the SSE event to process
   * @param httpRequest the HTTP request that initiated the SSE connection (can be null)
   * @param context additional context for template processing
   * @return the processed SSE event, or the original event if no template is configured or
   *     processing fails
   */
  public SseEvent processEventWithTemplate(
      SseEvent sseEvent, HttpRequest httpRequest, Map<String, Object> context) {
    Preconditions.checkNotNull(sseEvent, "sseEvent must not be null.");
    Preconditions.checkNotNull(httpRequest, "httpRequest must not be null.");
    if (exchangeTemplate == null || exchangeTemplate.trim().isEmpty() || templateManager == null) {
      return sseEvent;
    }

    try {
      // Create an SseExchange for template processing
      SseExchange sseExchange = new SseExchange(httpRequest, sseEvent);

      // Process the exchange using the template
      String resolvedTemplate =
          templateManager.resolveTemplate(sseExchange, exchangeTemplate, context);
      SseEvent response = sseExchange.getResponse();
      // Propagate attributes from the exchange to the SSE event
      if (response != null && !Strings.isNullOrEmpty(resolvedTemplate)) {
        sseExchange.setContent(resolvedTemplate);
      }
      return sseExchange.getResponse();
    } catch (Exception e) {
      LOGGER.warn("Failed to apply template processing to SSE event: {}", e.getMessage());
      return sseEvent;
    }
  }
}
