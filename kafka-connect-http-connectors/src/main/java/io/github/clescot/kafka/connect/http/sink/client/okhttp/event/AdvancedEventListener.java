package io.github.clescot.kafka.connect.http.sink.client.okhttp.event;


import io.micrometer.common.lang.NonNullApi;
import io.micrometer.common.lang.NonNullFields;
import io.micrometer.common.lang.Nullable;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.http.Outcome;
import okhttp3.EventListener;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * fork of the {@link io.micrometer.core.instrument.binder.okhttp3.OkHttpMetricsEventListener} eventListener implementation,
 * to add more details.
 * cf <a href="https://github.com/micrometer-metrics/micrometer/blob/main/micrometer-core/src/main/java/io/micrometer/core/instrument/binder/okhttp3/OkHttpMetricsEventListener.java">for the original code from the micrometer project</a>.
 */
@NonNullApi
@NonNullFields
public class AdvancedEventListener extends EventListener {

    /**
     * Header name for URI patterns which will be used for tag values.
     */
    public static final String URI_PATTERN = "URI_PATTERN";

    private static final boolean REQUEST_TAG_CLASS_EXISTS;

    public static final String OKHTTP_REQUEST_METRIC_NAME = "okhttp";
    public static final String OKHTTP_REQUEST_METRIC_DESCRIPTION = "Timer of OkHttp call";

    public static final String OKHTTP_POOL_CONNECTION_METRIC_NAME = "okhttp.pool.connection";
    public static final String OKHTTP_POOL_CONNECTION_METRIC_DESCRIPTION = "Timer of OkHttp connection acquisition from the pool";

    public static final String OKHTTP_SOCKET_CONNECTION_METRIC_NAME = "okhttp.socket.connection";
    public static final String OKHTTP_SOCKET_CONNECTION_METRIC_DESCRIPTION = "Timer of OkHttp socket acquisition";

    public static final String OKHTTP_DNS_METRIC_NAME = "okhttp.dns";
    public static final String OKHTTP_DNS_METRIC_DESCRIPTION = "Timer of OkHttp dns";

    public static final String OKHTTP_PROXY_SELECT_METRIC_NAME = "okhttp.proxyselect";
    public static final String OKHTTP_PROXY_SELECT_METRIC_DESCRIPTION = "Timer of OkHttp proxy select";

    public static final String OKHTTP_SECURE_CONNECT_METRIC_NAME = "okhttp.secureconnect";
    public static final String OKHTTP_SECURE_CONNECT_METRIC_DESCRIPTION = "Timer of OkHttp secure connection";

    public static final String OKHTTP_REQUEST_HEADERS_METRIC_NAME = "okhttp.requests.headers";
    public static final String OKHTTP_REQUEST_HEADERS_METRIC_DESCRIPTION = "Timer of OkHttp request headers";

    public static final String OKHTTP_REQUEST_BODY_METRIC_NAME = "okhttp.requests.body";
    public static final String OKHTTP_REQUEST_BODY_METRIC_DESCRIPTION = "Timer of OkHttp request body";

    public static final String OKHTTP_RESPONSE_HEADERS_METRIC_NAME = "okhttp.response.headers";
    public static final String OKHTTP_RESPONSE_HEADERS_METRIC_DESCRIPTION = "Timer of OkHttp response headers";

    public static final String OKHTTP_RESPONSE_BODY_METRIC_NAME = "okhttp.response.body";
    public static final String OKHTTP_RESPONSE_BODY_METRIC_DESCRIPTION = "Timer of OkHttp response body";


    static {
        REQUEST_TAG_CLASS_EXISTS = getMethod(Class.class) != null;
    }

    private static final String TAG_TARGET_SCHEME = "target.scheme";

    private static final String TAG_TARGET_HOST = "target.host";

    private static final String TAG_TARGET_PORT = "target.port";

    private static final String TAG_VALUE_UNKNOWN = "UNKNOWN";

    private static final Tags TAGS_TARGET_UNKNOWN = Tags.of(TAG_TARGET_SCHEME, TAG_VALUE_UNKNOWN, TAG_TARGET_HOST,
            TAG_VALUE_UNKNOWN, TAG_TARGET_PORT, TAG_VALUE_UNKNOWN);

    @Nullable
    private static Method getMethod(Class<?>... parameterTypes) {
        try {
            return Request.class.getMethod("tag", parameterTypes);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private final MeterRegistry registry;


    private final Function<Request, String> urlMapper;

    private final Iterable<Tag> extraTags;

    private final Iterable<BiFunction<Request, Response, Tag>> contextSpecificTags;

    private final Iterable<Tag> unknownRequestTags;

    private final boolean includeHostTag;

    // VisibleForTesting
    final ConcurrentMap<Call, AdvancedEventListener.CallState> callState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> dnsCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> connectionFromPoolCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> socketFromConnectionCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> proxySelectCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> secureConnectCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> requestHeadersCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> requestBodyCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> responseHeadersCallState = new ConcurrentHashMap<>();
    final ConcurrentMap<Call, AdvancedEventListener.CallState> responseBodyCallState = new ConcurrentHashMap<>();

    protected AdvancedEventListener(MeterRegistry registry,
                                    Function<Request, String> urlMapper, Iterable<Tag> extraTags,
                                    Iterable<BiFunction<Request, Response, Tag>> contextSpecificTags) {
        this(registry, urlMapper, extraTags, contextSpecificTags, emptyList(), true);
    }

    protected AdvancedEventListener(MeterRegistry registry, Function<Request, String> urlMapper,
                                    Iterable<Tag> extraTags, Iterable<BiFunction<Request, Response, Tag>> contextSpecificTags,
                                    Iterable<String> requestTagKeys, boolean includeHostTag) {
        this.registry = registry;
        this.urlMapper = urlMapper;
        this.extraTags = extraTags;
        this.contextSpecificTags = contextSpecificTags;
        this.includeHostTag = includeHostTag;

        List<Tag> unknownRequestTags = new ArrayList<>();
        for (String requestTagKey : requestTagKeys) {
            unknownRequestTags.add(Tag.of(requestTagKey, "UNKNOWN"));
        }
        this.unknownRequestTags = unknownRequestTags;
    }

    public static AdvancedEventListener.Builder builder(MeterRegistry registry) {
        return new AdvancedEventListener.Builder(registry);
    }

    @Override
    public void callStart(Call call) {
        callState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void callFailed(Call call, IOException e) {
        AdvancedEventListener.CallState state = callState.remove(call);
        if (state != null) {
            state.exception = e;
            time(state, OKHTTP_REQUEST_METRIC_NAME, OKHTTP_REQUEST_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void callEnd(Call call) {
        AdvancedEventListener.CallState state = callState.remove(call);
        if (state != null) {
            time(state, OKHTTP_REQUEST_METRIC_NAME, OKHTTP_REQUEST_METRIC_DESCRIPTION);
        }
    }


    // VisibleForTesting
    void time(AdvancedEventListener.CallState state, String metricName, String description) {
        Request request = state.request;
        boolean requestAvailable = request != null;

        Iterable<Tag> tags = getTags(state, requestAvailable, request);

        Timer.builder(metricName)
                .tags(tags)
                .description(description)
                .register(registry)
                .record(registry.config().clock().monotonicTime() - state.startTime, TimeUnit.NANOSECONDS);
    }

    @NotNull
    private Iterable<Tag> getTags(CallState state, boolean requestAvailable, Request request) {
        Iterable<Tag> tags = Tags
                .of("method", requestAvailable ? request.method() : TAG_VALUE_UNKNOWN, "uri", getUriTag(state, request),
                        "status", getStatusMessage(state.response, state.exception))
                .and(getStatusOutcome(state.response).asTag())
                .and(extraTags)
                .and(stream(contextSpecificTags.spliterator(), false)
                        .map(contextTag -> contextTag.apply(request, state.response))
                        .collect(toList()))
                .and(getRequestTags(request))
                .and(generateTagsForRoute(request));

        if (includeHostTag) {
            tags = Tags.of(tags).and("host", requestAvailable ? request.url().host() : TAG_VALUE_UNKNOWN);
        }
        return tags;
    }

    @NotNull
    private Iterable<Tag> getTags(CallState state) {
        return getTags(state, true, state.request);
    }

    private Tags generateTagsForRoute(@Nullable Request request) {
        if (request == null) {
            return TAGS_TARGET_UNKNOWN;
        }
        return Tags.of(TAG_TARGET_SCHEME, request.url().scheme(), TAG_TARGET_HOST, request.url().host(),
                TAG_TARGET_PORT, Integer.toString(request.url().port()));
    }

    private String getUriTag(AdvancedEventListener.CallState state, @Nullable Request request) {
        if (request == null) {
            return TAG_VALUE_UNKNOWN;
        }
        return state.response != null && (state.response.code() == 404 || state.response.code() == 301) ? "NOT_FOUND"
                : urlMapper.apply(request);
    }

    private Iterable<Tag> getRequestTags(@Nullable Request request) {
        if (request == null) {
            return unknownRequestTags;
        }
        if (REQUEST_TAG_CLASS_EXISTS) {
            Tags requestTag = request.tag(Tags.class);
            if (requestTag != null) {
                return requestTag;
            }
        }
        Object requestTag = request.tag();
        if (requestTag instanceof Tags) {
            return (Tags) requestTag;
        }
        return Tags.empty();
    }

    private Outcome getStatusOutcome(@Nullable Response response) {
        if (response == null) {
            return Outcome.UNKNOWN;
        }

        return Outcome.forStatus(response.code());
    }

    private String getStatusMessage(@Nullable Response response, @Nullable IOException exception) {
        if (exception != null) {
            return "IO_ERROR";
        }

        if (response == null) {
            return "CLIENT_ERROR";
        }

        return Integer.toString(response.code());
    }

    @Override
    public void cacheConditionalHit(@NotNull Call call, @NotNull Response cachedResponse) {
        CallState myCallState = new CallState(0L, call.request());
        Counter.builder("okhttp.cache.conditionalhits")
                .tags(getTags(myCallState))
                .description("counter of cache conditional hits")
                .register(registry)
                .increment();
    }

    @Override
    public void cacheHit(@NotNull Call call, @NotNull Response response) {
        CallState myCallState = new CallState(0L, call.request());
        Counter.builder("okhttp.cache.hits")
                .tags(getTags(myCallState))
                .description("counter of cache hits")
                .register(registry)
                .increment();
    }

    @Override
    public void cacheMiss(@NotNull Call call) {
        CallState myCallState = new CallState(0L, call.request());
        Counter.builder("okhttp.cache.misses")
                .tags(getTags(myCallState))
                .description("counter of cache misses")
                .register(registry)
                .increment();
    }

    @Override
    public void canceled(@NotNull Call call) {

        CallState stateCall = callState.remove(call);
        if (stateCall != null) {
            time(stateCall, OKHTTP_REQUEST_METRIC_NAME, OKHTTP_REQUEST_METRIC_DESCRIPTION);
        }
        CallState stateDns = dnsCallState.remove(call);
        if (stateDns != null) {
            time(stateDns, OKHTTP_DNS_METRIC_NAME, OKHTTP_DNS_METRIC_DESCRIPTION);
        }
        CallState stateConnection = connectionFromPoolCallState.remove(call);
        if (stateConnection != null) {
            time(stateConnection, OKHTTP_POOL_CONNECTION_METRIC_NAME, OKHTTP_POOL_CONNECTION_METRIC_DESCRIPTION);
        }
        CallState stateSocket = socketFromConnectionCallState.remove(call);
        if (stateSocket != null) {
            time(stateSocket, OKHTTP_SOCKET_CONNECTION_METRIC_NAME, OKHTTP_SOCKET_CONNECTION_METRIC_DESCRIPTION);
        }
        CallState stateProxySelect = proxySelectCallState.remove(call);
        if (stateProxySelect != null) {
            time(stateProxySelect, OKHTTP_PROXY_SELECT_METRIC_NAME, OKHTTP_PROXY_SELECT_METRIC_DESCRIPTION);
        }
        CallState stateSecureConnect = secureConnectCallState.remove(call);
        if (stateSecureConnect != null) {
            time(stateSecureConnect, OKHTTP_SECURE_CONNECT_METRIC_NAME, OKHTTP_SECURE_CONNECT_METRIC_DESCRIPTION);
        }
        CallState stateRequestHeaders = requestHeadersCallState.remove(call);
        if (stateRequestHeaders != null) {
            time(stateRequestHeaders, OKHTTP_RESPONSE_HEADERS_METRIC_NAME, OKHTTP_RESPONSE_HEADERS_METRIC_DESCRIPTION);
        }
        CallState stateRequestBody = requestBodyCallState.remove(call);
        if (stateRequestBody != null) {
            time(stateRequestBody, OKHTTP_REQUEST_BODY_METRIC_NAME, OKHTTP_REQUEST_BODY_METRIC_DESCRIPTION);
        }
        CallState stateResponseHeaders = responseHeadersCallState.remove(call);
        if (stateResponseHeaders != null) {
            time(stateResponseHeaders, OKHTTP_RESPONSE_HEADERS_METRIC_NAME, OKHTTP_RESPONSE_HEADERS_METRIC_DESCRIPTION);
        }
        CallState stateResponseBody = responseBodyCallState.remove(call);
        if (stateResponseBody != null) {
            time(stateResponseBody, OKHTTP_RESPONSE_BODY_METRIC_NAME, OKHTTP_RESPONSE_BODY_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void connectEnd(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy, @org.jetbrains.annotations.Nullable Protocol protocol) {
        AdvancedEventListener.CallState state = socketFromConnectionCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_SOCKET_CONNECTION_METRIC_NAME, OKHTTP_SOCKET_CONNECTION_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void connectFailed(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy, @org.jetbrains.annotations.Nullable Protocol protocol, @NotNull IOException ioe) {
        AdvancedEventListener.CallState state = socketFromConnectionCallState.remove(call);
        if (state != null) {
            state.exception = ioe;
            time(state, OKHTTP_SOCKET_CONNECTION_METRIC_NAME, OKHTTP_SOCKET_CONNECTION_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void connectStart(@NotNull Call call, @NotNull InetSocketAddress inetSocketAddress, @NotNull Proxy proxy) {
        socketFromConnectionCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void connectionAcquired(@NotNull Call call, @NotNull Connection connection) {
        connectionFromPoolCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void connectionReleased(@NotNull Call call, @NotNull Connection connection) {
        AdvancedEventListener.CallState state = connectionFromPoolCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_POOL_CONNECTION_METRIC_NAME, OKHTTP_POOL_CONNECTION_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void dnsEnd(@NotNull Call call, @NotNull String domainName, @NotNull List<InetAddress> inetAddressList) {
        AdvancedEventListener.CallState state = dnsCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_DNS_METRIC_NAME, OKHTTP_DNS_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void dnsStart(@NotNull Call call, @NotNull String domainName) {
        dnsCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void proxySelectEnd(@NotNull Call call, @NotNull HttpUrl url, @NotNull List<Proxy> proxies) {
        AdvancedEventListener.CallState state = proxySelectCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_PROXY_SELECT_METRIC_NAME, OKHTTP_PROXY_SELECT_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void proxySelectStart(@NotNull Call call, @NotNull HttpUrl url) {
        proxySelectCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void requestBodyEnd(@NotNull Call call, long byteCount) {
        AdvancedEventListener.CallState state = requestBodyCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_REQUEST_BODY_METRIC_NAME, OKHTTP_REQUEST_BODY_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void requestBodyStart(@NotNull Call call) {
        requestBodyCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void requestFailed(@NotNull Call call, @NotNull IOException ioe) {
        AdvancedEventListener.CallState stateHeaders = requestHeadersCallState.remove(call);
        if (stateHeaders != null) {
            stateHeaders.exception = ioe;
            time(stateHeaders, OKHTTP_REQUEST_HEADERS_METRIC_NAME, OKHTTP_REQUEST_HEADERS_METRIC_DESCRIPTION);
        }
        AdvancedEventListener.CallState stateBody = requestBodyCallState.remove(call);
        if (stateBody != null) {
            stateBody.exception = ioe;
            time(stateBody, OKHTTP_REQUEST_BODY_METRIC_NAME, OKHTTP_REQUEST_BODY_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void requestHeadersEnd(@NotNull Call call, @NotNull Request request) {
        AdvancedEventListener.CallState state = requestHeadersCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_REQUEST_HEADERS_METRIC_NAME, OKHTTP_REQUEST_HEADERS_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void requestHeadersStart(@NotNull Call call) {
        requestHeadersCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void responseBodyEnd(@NotNull Call call, long byteCount) {
        AdvancedEventListener.CallState state = responseBodyCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_RESPONSE_BODY_METRIC_NAME, OKHTTP_RESPONSE_BODY_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void responseBodyStart(@NotNull Call call) {
        responseBodyCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void responseFailed(@NotNull Call call, @NotNull IOException ioe) {
        AdvancedEventListener.CallState stateHeaders = responseHeadersCallState.remove(call);
        if (stateHeaders != null) {
            stateHeaders.exception = ioe;
            time(stateHeaders, OKHTTP_RESPONSE_HEADERS_METRIC_NAME, OKHTTP_RESPONSE_HEADERS_METRIC_DESCRIPTION);
        }
        AdvancedEventListener.CallState stateBody = responseBodyCallState.remove(call);
        if (stateBody != null) {
            stateBody.exception = ioe;
            time(stateBody, OKHTTP_RESPONSE_BODY_METRIC_NAME, OKHTTP_RESPONSE_BODY_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void responseHeadersStart(@NotNull Call call) {
        responseHeadersCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void responseHeadersEnd(Call call, Response response) {
        AdvancedEventListener.CallState state = responseHeadersCallState.remove(call);
        if (state != null) {
            state.response = response;
            time(state, OKHTTP_RESPONSE_HEADERS_METRIC_NAME, OKHTTP_RESPONSE_HEADERS_METRIC_DESCRIPTION);
        }
    }

    @Override
    public void satisfactionFailure(@NotNull Call call, @NotNull Response response) {
        //TODO add counter or DistributionSummary ?
        super.satisfactionFailure(call, response);
    }

    @Override
    public void secureConnectStart(@NotNull Call call) {
        secureConnectCallState.put(call, new AdvancedEventListener.CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    @Override
    public void secureConnectEnd(@NotNull Call call, @org.jetbrains.annotations.Nullable Handshake handshake) {
        AdvancedEventListener.CallState state = responseHeadersCallState.remove(call);
        if (state != null) {
            time(state, OKHTTP_SECURE_CONNECT_METRIC_NAME, OKHTTP_SECURE_CONNECT_METRIC_DESCRIPTION);
        }
    }

    // VisibleForTesting
    static class CallState {

        final long startTime;

        @Nullable
        final Request request;

        @Nullable
        Response response;

        @Nullable
        IOException exception;

        CallState(long startTime, @Nullable Request request) {
            this.startTime = startTime;
            this.request = request;
        }

    }

    public static class Builder {

        private final MeterRegistry registry;


        private Function<Request, String> uriMapper = (request) -> Optional.ofNullable(request.header(URI_PATTERN))
                .orElse("none");

        private Tags tags = Tags.empty();

        private Collection<BiFunction<Request, Response, Tag>> contextSpecificTags = new ArrayList<>();

        private boolean includeHostTag = true;

        private Iterable<String> requestTagKeys = Collections.emptyList();

        Builder(MeterRegistry registry) {
            this.registry = registry;
        }

        public AdvancedEventListener.Builder tags(Iterable<Tag> tags) {
            this.tags = this.tags.and(tags);
            return this;
        }

        /**
         * Add a {@link Tag} to any already configured tags on this Builder.
         *
         * @param tag tag to add
         * @return this builder
         * @since 1.5.0
         */
        public AdvancedEventListener.Builder tag(Tag tag) {
            this.tags = this.tags.and(tag);
            return this;
        }

        /**
         * Add a context-specific tag.
         *
         * @param contextSpecificTag function to create a context-specific tag
         * @return this builder
         * @since 1.5.0
         */
        public AdvancedEventListener.Builder tag(BiFunction<Request, Response, Tag> contextSpecificTag) {
            this.contextSpecificTags.add(contextSpecificTag);
            return this;
        }

        public AdvancedEventListener.Builder uriMapper(Function<Request, String> uriMapper) {
            this.uriMapper = uriMapper;
            return this;
        }

        /**
         * Historically, OkHttp Metrics provided by {@link io.micrometer.core.instrument.binder.okhttp3.OkHttpMetricsEventListener}
         * included a {@code host} tag for the target host being called. To align with
         * other HTTP client metrics, this was changed to {@code target.host}, but to
         * maintain backwards compatibility the {@code host} tag can also be included. By
         * default, {@code includeHostTag} is {@literal true} so both tags are included.
         *
         * @param includeHostTag whether to include the {@code host} tag
         * @return this builder
         * @since 1.5.0
         */
        public AdvancedEventListener.Builder includeHostTag(boolean includeHostTag) {
            this.includeHostTag = includeHostTag;
            return this;
        }

        /**
         * Tag keys for {@link Request#tag()} or {@link Request#tag(Class)}.
         * <p>
         * These keys will be added with {@literal UNKNOWN} values when {@link Request} is
         * {@literal null}. Note that this is required only for Prometheus as it requires
         * tag match for the same metric.
         *
         * @param requestTagKeys request tag keys
         * @return this builder
         * @since 1.3.9
         */
        public AdvancedEventListener.Builder requestTagKeys(String... requestTagKeys) {
            return requestTagKeys(Arrays.asList(requestTagKeys));
        }

        /**
         * Tag keys for {@link Request#tag()} or {@link Request#tag(Class)}.
         * <p>
         * These keys will be added with {@literal UNKNOWN} values when {@link Request} is
         * {@literal null}. Note that this is required only for Prometheus as it requires
         * tag match for the same metric.
         *
         * @param requestTagKeys request tag keys
         * @return this builder
         * @since 1.3.9
         */
        public AdvancedEventListener.Builder requestTagKeys(Iterable<String> requestTagKeys) {
            this.requestTagKeys = requestTagKeys;
            return this;
        }

        public AdvancedEventListener build() {
            return new AdvancedEventListener(registry, uriMapper, tags, contextSpecificTags, requestTagKeys,
                    includeHostTag);
        }

    }

}
