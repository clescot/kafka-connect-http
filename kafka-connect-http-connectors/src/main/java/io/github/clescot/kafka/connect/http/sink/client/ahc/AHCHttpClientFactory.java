package io.github.clescot.kafka.connect.http.sink.client.ahc;

import io.github.clescot.kafka.connect.http.sink.client.HttpClient;
import io.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class AHCHttpClientFactory implements HttpClientFactory<Request, Response> {




    private final static Logger LOGGER = LoggerFactory.getLogger(AHCHttpClientFactory.class);





    @Override
    public HttpClient<Request, Response> build(Map<String, Object> config,ExecutorService executorService) {
        //executorService is not used for AHC : we cannot set an executorService nor a thread pool to AHC
        return new AHCHttpClient(config);
    }

}

