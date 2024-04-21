package io.github.clescot.kafka.connect.http.client.okhttp;

import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.http.HTTPRequestSender;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.http.ReadOnlyHTTPRequest;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import okhttp3.OkHttpClient;
import okhttp3.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class OkHttpHTTPRequestSender implements HTTPRequestSender {

    public static final String CONTENT_TYPE = "Content-Type";
    private final OkHttpClient okHttpClient;

    public OkHttpHTTPRequestSender(OkHttpClient okHttpClient) {
        this.okHttpClient = okHttpClient;
    }

    private Request toNative(ReadOnlyHTTPRequest httpRequest){
        Request.Builder builder = new Request.Builder();
        Map<String, List<String>> headerMap = httpRequest.getHeaderMap();
        Map<String, String> myHeaders = headerMap.entrySet().stream().map(entry -> Map.entry(entry.getKey(), !entry.getValue().isEmpty() ? entry.getValue().get(0) : "")).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Headers headers = Headers.of(myHeaders);
        MediaType contentType = MediaType.get(httpRequest.getHeaderMap().get(CONTENT_TYPE).get(0));
        return builder.url(httpRequest.getURL())
                .method(httpRequest.getMethod().name(), RequestBody.create(httpRequest.getBody(), contentType))
                .headers(headers)
                .build();
    }
    private HttpRequest toRequest(ReadOnlyHTTPRequest httpRequest){
        HttpRequest request = new HttpRequest(httpRequest.getURL().toString(),httpRequest.getMethod().name());
        request.setHeaders(httpRequest.getHeaderMap());
        return request;
    }

    private HTTPResponse fromResponse(HttpResponse response){
        HTTPResponse nimbusResponse = new HTTPResponse(response.getStatusCode());
        nimbusResponse.setStatusMessage(response.getStatusMessage());
        try {
            nimbusResponse.setContentType(response.getResponseHeaders().get(CONTENT_TYPE).get(0));
            response.getResponseHeaders().entrySet().forEach(entry-> nimbusResponse.setHeader(entry.getKey(),entry.getValue().toArray(new String[entry.getValue().size()])));
            nimbusResponse.setBody(response.getResponseBody());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return nimbusResponse;
    }

    @Override
    public HTTPResponse send(ReadOnlyHTTPRequest httpRequest) throws IOException {
        Request nativeRequest = toNative(httpRequest);
        Response response = okHttpClient.newCall(nativeRequest).execute();
        HTTPResponse nimbusResponse = toNimbusResponse(response);
        return nimbusResponse;
    }

    public HTTPResponse send2(HttpClient httpClient, ReadOnlyHTTPRequest nimbusRequest) throws ExecutionException, InterruptedException {
        HttpRequest httpRequest = toRequest(nimbusRequest);
        CompletableFuture<HttpExchange> call = httpClient.call(httpRequest, new AtomicInteger(1));
        HttpExchange httpExchange = call.get();
        HTTPResponse nimbusResponse = fromResponse(httpExchange.getHttpResponse());
        return nimbusResponse;
    }

    private HTTPResponse toNimbusResponse(Response response){
        HTTPResponse httpResponse = new HTTPResponse(response.code());
        httpResponse.setStatusMessage(response.message());
        try {
        httpResponse.setContentType(response.header(CONTENT_TYPE));
        response.headers().forEach(pair->httpResponse.setHeader(pair.getFirst(),pair.getSecond()));

            httpResponse.setBody(response.body().string());
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
        return httpResponse;
    }
}
