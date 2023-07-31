package io.github.clescot.kafka.connect.http.sink.client.okhttp.event;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import okhttp3.Call;
import okhttp3.EventListener;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.client.okhttp.event.AdvancedEventListener.URI_PATTERN;

public class AdvancedEventListenerFactory implements EventListener.Factory {

    private final MeterRegistry meterRegistry;
    private boolean includeHostTag;
    private final boolean includUrlPath;
    private final String[] tags;

    public AdvancedEventListenerFactory(MeterRegistry meterRegistry,boolean includeHostTag,boolean includUrlPath,String... tags) {
        this.meterRegistry = meterRegistry;
        this.includeHostTag = includeHostTag;
        this.includUrlPath = includUrlPath;
        this.tags = tags;
    }

    @NotNull
    @Override
    public EventListener create(@NotNull Call call) {
        includeHostTag = true;
        return AdvancedEventListener.builder(meterRegistry)
                .uriMapper(includUrlPath?
                        req -> req.url().encodedPath()
                        :(request) -> Optional.ofNullable(request.header(URI_PATTERN)).orElse("none"))
                .tags(Tags.of(tags))
                .includeHostTag(includeHostTag)
                .requestTagKeys()
                .build();
    }
}
