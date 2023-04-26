package io.github.clescot.kafka.connect.http.sink.client;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.micrometer.common.lang.NonNullApi;
import io.micrometer.common.lang.NonNullFields;
import io.micrometer.common.lang.Nullable;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import okhttp3.Call;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
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
 * <a href="https://github.com/micrometer-metrics/micrometer/blob/main/micrometer-core/src/main/java/io/micrometer/core/instrument/binder/okhttp3/OkHttpMetricsEventListener.java">from OkHttpMetricsEventListener</a>
 *
 *
 * {@link EventListener} for collecting metrics from {@link HttpClient}.
 * <p>
 * {@literal uri} tag is usually limited to URI patterns to mitigate tag cardinality
 * explosion but {@link HttpClient} doesn't provide URI patterns. We provide
 * {@value io.micrometer.core.instrument.binder.okhttp3.OkHttpMetricsEventListener#URI_PATTERN} header to support {@literal uri} tag or
 * you can configure a {@link io.micrometer.core.instrument.binder.okhttp3.OkHttpMetricsEventListener.Builder#uriMapper(Function) URI mapper} to provide your own
 * tag values for {@literal uri} tag.
 *
 * @author Bjarte S. Karlsen
 * @author Jon Schneider
 * @author Nurettin Yilmaz
 * @author Johnny Lim
 */
@NonNullApi
@NonNullFields
public class HttpEventListener {

    /**
     * Header name for URI patterns which will be used for tag values.
     */
    public static final String URI_PATTERN = "URI_PATTERN";


    private static final String TAG_TARGET_SCHEME = "target.scheme";

    private static final String TAG_TARGET_HOST = "target.host";

    private static final String TAG_TARGET_PORT = "target.port";

    private static final String TAG_VALUE_UNKNOWN = "UNKNOWN";

    private static final Tags TAGS_TARGET_UNKNOWN = Tags.of(TAG_TARGET_SCHEME, TAG_VALUE_UNKNOWN, TAG_TARGET_HOST,
            TAG_VALUE_UNKNOWN, TAG_TARGET_PORT, TAG_VALUE_UNKNOWN);


    private final MeterRegistry registry;

    private final String requestsMetricName;

    private final Function<HttpRequest, String> urlMapper;

    private final Iterable<Tag> extraTags;

    private final Iterable<BiFunction<HttpRequest, HttpResponse, Tag>> contextSpecificTags;

    private final Iterable<Tag> unknownRequestTags;

    private final boolean includeHostTag;

    // VisibleForTesting
    final ConcurrentMap<Call, CallState> callStates = new ConcurrentHashMap<>();

    protected HttpEventListener(MeterRegistry registry, String requestsMetricName,
                                Function<HttpRequest, String> urlMapper, Iterable<Tag> extraTags,
                                Iterable<BiFunction<HttpRequest, HttpResponse, Tag>> contextSpecificTags) {
        this(registry, requestsMetricName, urlMapper, extraTags, contextSpecificTags, emptyList(), true);
    }

    HttpEventListener(MeterRegistry registry, String requestsMetricName, Function<HttpRequest, String> urlMapper,
                      Iterable<Tag> extraTags, Iterable<BiFunction<HttpRequest, HttpResponse, Tag>> contextSpecificTags,
                      Iterable<String> requestTagKeys, boolean includeHostTag) {
        this.registry = registry;
        this.requestsMetricName = requestsMetricName;
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

    public static HttpEventListener.Builder builder(MeterRegistry registry, String name) {
        return new HttpEventListener.Builder(registry, name);
    }

    public void callStart(Call call) {
        //TODO JMX
//        callStates.put(call, new CallState(registry.config().clock().monotonicTime(), call.request()));
    }

    public void callFailed(Call call, IOException e) {
        CallState state = callStates.remove(call);
        if (state != null) {
            state.exception = e;
            time(state);
        }
    }

    public void callEnd(Call call) {
        callStates.remove(call);
    }

    public void responseHeadersEnd(Call call, HttpResponse response) {
        CallState state = callStates.remove(call);
        if (state != null) {
            state.response = response;
            time(state);
        }
    }

    // VisibleForTesting
    void time(CallState state) {
        HttpRequest request = state.request;
        boolean requestAvailable = request != null;

        Iterable<Tag> tags = Tags
                .of("method", requestAvailable ? request.getMethod() : TAG_VALUE_UNKNOWN, "uri", getUriTag(state, request),
                        "status", getStatusMessage(state.response, state.exception))
                .and(extraTags)
                .and(stream(contextSpecificTags.spliterator(), false)
                        .map(contextTag -> contextTag.apply(request, state.response)).collect(toList()))
                .and(getRequestTags(request)).and(generateTagsForRoute(request));

        if (includeHostTag) {
            String urlAsString = request.getUrl();
            URL url;
            try {
                url = new URL(urlAsString);
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
            tags = Tags.of(tags).and("host", requestAvailable ? url.getHost() : TAG_VALUE_UNKNOWN);
        }

        Timer.builder(this.requestsMetricName).tags(tags).description("Timer of OkHttp operation").register(registry)
                .record(registry.config().clock().monotonicTime() - state.startTime, TimeUnit.NANOSECONDS);
    }


    private Tags generateTagsForRoute(@Nullable HttpRequest request) {
        if (request == null) {
            return TAGS_TARGET_UNKNOWN;
        }
        String urlAsString = request.getUrl();
        URL url;
        try {
            url = new URL(urlAsString);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        return Tags.of(TAG_TARGET_SCHEME, url.getProtocol(), TAG_TARGET_HOST, url.getHost(),
                TAG_TARGET_PORT, Integer.toString(url.getPort()));
    }

    private String getUriTag(CallState state, @Nullable HttpRequest request) {
        if (request == null) {
            return TAG_VALUE_UNKNOWN;
        }
        return state.response != null && (state.response.getStatusCode() == 404 || state.response.getStatusCode() == 301) ? "NOT_FOUND"
                : urlMapper.apply(request);
    }

    private Iterable<Tag> getRequestTags(@Nullable HttpRequest request) {
        if (request == null) {
            return unknownRequestTags;
        }

        Tags requestTag = request.getTag(Tags.class).orElse(null);
        if (requestTag != null) {
            return requestTag;
        }

        return Tags.empty();
    }

    private String getStatusMessage(@Nullable HttpResponse response, @Nullable IOException exception) {
        if (exception != null) {
            return "IO_ERROR";
        }

        if (response == null) {
            return "CLIENT_ERROR";
        }

        return Integer.toString(response.getStatusCode());
    }


    public static class Builder {

        private final MeterRegistry registry;

        private final String name;

        private Function<HttpRequest, String> uriMapper = (request) -> {
            Map<String, List<String>> headers = request.getHeaders();
            return Optional.ofNullable(headers.get(URI_PATTERN) != null && headers.get(URI_PATTERN).size() > 0 ? headers.get(URI_PATTERN).get(0) : null)
                    .orElse("none");
        };

        private Tags tags = Tags.empty();

        private Collection<BiFunction<HttpRequest, HttpResponse, Tag>> contextSpecificTags = new ArrayList<>();

        private boolean includeHostTag = true;

        private Iterable<String> requestTagKeys = Collections.emptyList();

        Builder(MeterRegistry registry, String name) {
            this.registry = registry;
            this.name = name;
        }

        public HttpEventListener.Builder tags(Iterable<Tag> tags) {
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
        public HttpEventListener.Builder tag(Tag tag) {
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
        public HttpEventListener.Builder tag(BiFunction<HttpRequest, HttpResponse, Tag> contextSpecificTag) {
            this.contextSpecificTags.add(contextSpecificTag);
            return this;
        }

        public HttpEventListener.Builder uriMapper(Function<HttpRequest, String> uriMapper) {
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
        public HttpEventListener.Builder includeHostTag(boolean includeHostTag) {
            this.includeHostTag = includeHostTag;
            return this;
        }

        /**
         * Tag keys for {@link HttpRequest#getTag(Class)}.
         * <p>
         * These keys will be added with {@literal UNKNOWN} values when {@link HttpRequest} is
         * {@literal null}. Note that this is required only for Prometheus as it requires
         * tag match for the same metric.
         *
         * @param requestTagKeys request tag keys
         * @return this builder
         * @since 1.3.9
         */
        public HttpEventListener.Builder requestTagKeys(String... requestTagKeys) {
            return requestTagKeys(Arrays.asList(requestTagKeys));
        }

        /**
         * Tag keys for {@link HttpRequest#getTag(Class)}.
         * <p>
         * These keys will be added with {@literal UNKNOWN} values when {@link HttpRequest} is
         * {@literal null}. Note that this is required only for Prometheus as it requires
         * tag match for the same metric.
         *
         * @param requestTagKeys request tag keys
         * @return this builder
         * @since 1.3.9
         */
        public HttpEventListener.Builder requestTagKeys(Iterable<String> requestTagKeys) {
            this.requestTagKeys = requestTagKeys;
            return this;
        }

        public HttpEventListener build() {
            return new HttpEventListener(registry, name, uriMapper, tags, contextSpecificTags, requestTagKeys,
                    includeHostTag);
        }

    }

}



