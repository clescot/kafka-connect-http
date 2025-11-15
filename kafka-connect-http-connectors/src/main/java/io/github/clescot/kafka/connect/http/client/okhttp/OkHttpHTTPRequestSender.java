package io.github.clescot.kafka.connect.http.client.okhttp;

import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.http.HTTPRequestSender;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.http.ReadOnlyHTTPRequest;
import okhttp3.*;
import okhttp3.OkHttpClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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


    @Override
    public HTTPResponse send(ReadOnlyHTTPRequest httpRequest) throws IOException {
        Request nativeRequest = toNative(httpRequest);
        Response response = okHttpClient.newCall(nativeRequest).execute();
        return toNimbusResponse(response);
    }


    private HTTPResponse toNimbusResponse(Response response){
        HTTPResponse httpResponse = new HTTPResponse(response.code());
        httpResponse.setStatusMessage(response.message());
        try {
        httpResponse.setContentType(Optional.ofNullable(response.header(CONTENT_TYPE)).orElse("application/json"));
        response.headers().forEach(pair->httpResponse.setHeader(pair.getFirst(),pair.getSecond()));
            if(response.body()!=null) {
                httpResponse.setBody(response.body().string());
            }
        } catch (IOException | ParseException e) {
            throw new IllegalStateException(e);
        }
        return httpResponse;
    }
}
