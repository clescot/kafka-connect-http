package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import okhttp3.Request;
import okhttp3.Response;

public class OkHttpSinkTask extends HttpSinkTask<Request, Response> {

    public OkHttpSinkTask() {
        super(new OkHttpClientFactory());
    }

    protected OkHttpSinkTask(boolean mock) {
        super(new OkHttpClientFactory(), mock);
    }
}
