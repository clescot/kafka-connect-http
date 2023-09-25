package io.github.clescot.kafka.connect.http.client.okhttp.event;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import okhttp3.Call;
import okhttp3.EventListener;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

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
                        :(request) -> Optional.ofNullable(request.header(AdvancedEventListener.URI_PATTERN)).orElse("none"))
                .tags(Tags.of(tags))
                .includeHostTag(includeHostTag)
                .requestTagKeys()
                .build();
    }
}
