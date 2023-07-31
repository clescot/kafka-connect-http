package io.github.clescot.kafka.connect.http.sink.client.okhttp.event;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.okhttp3.OkHttpMetricsEventListener;
import okhttp3.Call;
import okhttp3.EventListener;
import org.jetbrains.annotations.NotNull;

public class AdvancedEventListenerFactory implements EventListener.Factory {

    private final MeterRegistry meterRegistry;

    public AdvancedEventListenerFactory(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @NotNull
    @Override
    public EventListener create(@NotNull Call call) {
        return OkHttpMetricsEventListener.builder(meterRegistry, "okhttp.requests")
                .uriMapper(req -> req.url().encodedPath())//beware of tag cardinality explosion => replace .encodedPath() with .host()
//                        .uriMapper(req -> req.url().host())
//                .tags(Tags.of("foo", "bar"))
                .includeHostTag(true)
                .requestTagKeys()
                .build();
    }
}
