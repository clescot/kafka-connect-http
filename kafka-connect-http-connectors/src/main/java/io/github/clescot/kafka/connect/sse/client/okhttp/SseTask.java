package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.Task;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.sse.core.SseEvent;
import okhttp3.Request;
import okhttp3.Response;

import java.util.List;
import java.util.Map;

import static io.github.clescot.kafka.connect.http.sink.HttpClientConfigDefinition.CONFIGURATION_IDS;
import static io.github.clescot.kafka.connect.http.sink.HttpClientConfigurationFactory.buildConfigurations;

public class SseTask implements Task<OkHttpClient,SseConfiguration,HttpRequest, SseEvent> {


    public SseTask(Map<String,String> settings,
                   HttpClientFactory<OkHttpClient, Request, Response> httpClientFactory) {

//        //configurations
//        List<HttpClientConfiguration<OkHttpClient, Request, Response>> httpClientConfigurations = buildConfigurations(
//                httpClientFactory,
//                executorService,
//                httpConnectorConfig.getList(CONFIGURATION_IDS),
//                httpConnectorConfig.originals(), meterRegistry
//        );
    }

    @Override
    public List<SseConfiguration> getConfigurations() {
        return List.of();
    }


}
