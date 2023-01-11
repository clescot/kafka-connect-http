package com.github.clescot.kafka.connect.http.sink.client.okhttp;

import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import okhttp3.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OkHttpClient implements HttpClient<Request, Response> {
    private Map<String, String> config;
    private okhttp3.OkHttpClient client;

    public OkHttpClient(Map<String, String> config) {
        this.config = config;
         client = new okhttp3.OkHttpClient();
    }

    @Override
    public  Request buildRequest(HttpRequest httpRequest) {
        //url
        String url = httpRequest.getUrl();
        HttpUrl okHttpUrl = HttpUrl.parse(url);

        //headers
        Headers.Builder okHeadersBuilder = new Headers.Builder();
        Map<String, List<String>> headers = httpRequest.getHeaders();
        headers.entrySet().forEach(entry -> {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            for (String value : values) {
                okHeadersBuilder.add(key,value);
            }
        });

        //
//        byte[] bodyAsByteArray = httpRequest.getBodyAsByteArray();
        RequestBody requestBody = null;
//        if(HttpRequest.BodyType.STRING.equals(httpRequest.getBodyType())){
//            String bodyAsString = httpRequest.getBodyAsString();
//            List<String> encodedNames = Lists.newArrayList();
//            List<String> encodedValues = Lists.newArrayList();
//            FormBody.Builder builder = new FormBody.Builder();
//            builder.add("string","value");
//            builder.addEncoded("string","value");
//            Request.Builder requestBuilder = new Request.Builder();
//            MediaType.get()
//            RequestBody body =RequestBody.create(json, JSON);
//            MediaType
//            requestBody = new FormBody(encodedNames,encodedValues);
//        }
        //TODO set requestBody from HttpRequest
        Request request = new Request(okHttpUrl,httpRequest.getMethod(),okHeadersBuilder.build(),requestBody, Maps.newHashMap());
        return request;
    }

    @Override
    public HttpResponse buildResponse(Response response) {
        HttpResponse httpResponse;
        try {
            httpResponse = new HttpResponse(response.code(),response.message(),response.body().string());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return httpResponse;
    }

    @Override
    public Response nativeCall(Request request) {
        Call call = client.newCall(request);
        try {
            return call.execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
