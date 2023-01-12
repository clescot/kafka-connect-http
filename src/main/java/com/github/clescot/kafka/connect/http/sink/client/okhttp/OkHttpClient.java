package com.github.clescot.kafka.connect.http.sink.client.okhttp;

import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import okhttp3.*;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        assert okHttpUrl != null;
        //headers
        Headers.Builder okHeadersBuilder = new Headers.Builder();
        Map<String, List<String>> headers = httpRequest.getHeaders();
        headers.entrySet().forEach(entry -> {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            for (String value : values) {
                okHeadersBuilder.add(key, value);
            }
        });
        List<String> contentType = headers.get("Content-Type");
        String firstContentType = null;
        if (contentType != null && !contentType.isEmpty()) {
            firstContentType = contentType.get(0);
        }
        RequestBody requestBody = null;
        if (HttpRequest.BodyType.STRING.equals(httpRequest.getBodyType())) {
            //use the contentType set in HttpRequest. if not set, use application/json
            requestBody = RequestBody.create(httpRequest.getBodyAsString(), MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/json")));
        } else if (HttpRequest.BodyType.BYTE_ARRAY.equals(httpRequest.getBodyType())) {
            String encoded = Base64.getEncoder().encodeToString(httpRequest.getBodyAsByteArray());
            requestBody = RequestBody.create(encoded, MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/octet-stream")));
        }if(HttpRequest.BodyType.FORM.equals(httpRequest.getBodyType())){
            FormBody.Builder builder = new FormBody.Builder();
            Map<String,String> multiparts = null;
            for (Map.Entry<String, String> entry : multiparts.entrySet()) {
                builder.add(entry.getKey(),entry.getValue());
            }
            requestBody = builder.build();
    }else{
            //HttpRequest.BodyType = MULTIPART
            if(firstContentType.startsWith("multipart/form-data")){
                List<byte[]> bodyAsMultipart = httpRequest.getBodyAsMultipart();

                //TODO handle multipart
                //List<String> encodedNames = Lists.newArrayList();
                //List<String> encodedValues = Lists.newArrayList();

                //builder.add("string","value");
                //builder.addEncoded("string","value");
            }
        }

        return new Request(okHttpUrl,httpRequest.getMethod(),okHeadersBuilder.build(),requestBody, Maps.newHashMap());
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
