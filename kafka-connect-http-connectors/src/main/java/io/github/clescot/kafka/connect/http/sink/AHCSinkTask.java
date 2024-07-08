package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.client.ahc.AHCHttpClientFactory;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

public class AHCSinkTask extends HttpSinkTask<Request, Response> {
    public AHCSinkTask() {
        super(new AHCHttpClientFactory());
    }

    protected AHCSinkTask(boolean mock) {
        super(new AHCHttpClientFactory(),mock);
    }
}
