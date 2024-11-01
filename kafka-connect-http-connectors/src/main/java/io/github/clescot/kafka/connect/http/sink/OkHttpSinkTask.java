package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.sink.publish.KafkaProducer;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.producer.MockProducer;

public class OkHttpSinkTask extends HttpSinkTask<Request, Response> {

    public OkHttpSinkTask() {
        super(new OkHttpClientFactory(), new KafkaProducer<>());
    }

    protected OkHttpSinkTask(MockProducer<String,Object> mockProducer) {
        super(new OkHttpClientFactory(),new KafkaProducer<>(mockProducer));
    }
}
