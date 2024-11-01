package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.client.ahc.AHCHttpClientFactory;
import io.github.clescot.kafka.connect.http.sink.publish.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

public class AHCSinkTask extends HttpSinkTask<Request, Response> {
    public AHCSinkTask() {
        super(new AHCHttpClientFactory(), new KafkaProducer<>());
    }

    protected AHCSinkTask(MockProducer<String,Object> mockProducer) {
        super(new AHCHttpClientFactory(),new KafkaProducer<>(mockProducer));
    }
}
