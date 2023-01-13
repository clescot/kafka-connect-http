package com.github.clescot.kafka.connect.http.sink.client.okhttp;

import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.github.clescot.kafka.connect.http.sink.client.AbstractHttpClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kotlin.Pair;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.util.*;

import static com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.HTTPCLIENT_PROTOCOLS;
import static com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.HTTPCLIENT_SSL_SKIP_HOSTNAME_VERIFICATION;

public class OkHttpClient extends AbstractHttpClient<Request, Response> {
    private static final String PROTOCOL_SEPARATOR = ",";
    private okhttp3.OkHttpClient client;
    private Logger LOGGER = LoggerFactory.getLogger(OkHttpClient.class);

    public OkHttpClient(Map<String, String> config) {
        super(config);
        okhttp3.OkHttpClient.Builder httpClientBuilder = new okhttp3.OkHttpClient.Builder();
        if(config.containsKey(HTTPCLIENT_PROTOCOLS)) {
            String protocolNames = config.get(HTTPCLIENT_PROTOCOLS);
            List<Protocol> protocols = Lists.newArrayList();
            List<String> strings = Lists.newArrayList(protocolNames.split(PROTOCOL_SEPARATOR));
            for (String protocolName : strings) {
                Protocol protocol = Protocol.valueOf(protocolName);
                protocols.add(protocol);
            }
            httpClientBuilder.protocols(protocols);
        }
        Optional<KeyManagerFactory> keyManagerFactoryOption = getKeyManagerFactory();
        Optional<TrustManagerFactory> trustManagerFactoryOption = getTrustManagerFactory();
        if(keyManagerFactoryOption.isPresent()||trustManagerFactoryOption.isPresent()) {
            SSLSocketFactory ssl = getSSLSocketFactory(keyManagerFactoryOption.orElse(null), trustManagerFactoryOption.orElse(null), "SSL");
            if(trustManagerFactoryOption.isPresent()) {
                TrustManager[] trustManagers = trustManagerFactoryOption.get().getTrustManagers();
                if(trustManagers.length>0) {
                    httpClientBuilder.sslSocketFactory(ssl, (X509TrustManager) trustManagers[0]);
                }
            }
            if(config.containsKey(HTTPCLIENT_SSL_SKIP_HOSTNAME_VERIFICATION) && Boolean.parseBoolean(config.get(HTTPCLIENT_SSL_SKIP_HOSTNAME_VERIFICATION))){
                httpClientBuilder.hostnameVerifier((hostname, session) -> true);
            }
        }
        client = httpClientBuilder.build();


    }

    @Override
    public Request buildRequest(HttpRequest httpRequest) {
        Request.Builder builder = new Request.Builder();

        //url
        String url = httpRequest.getUrl();
        HttpUrl okHttpUrl = HttpUrl.parse(url);
        builder.url(okHttpUrl);

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
        Headers okHeaders = okHeadersBuilder.build();
        builder.headers(okHeaders);
        //Content-Type
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
        }else if (HttpRequest.BodyType.FORM.equals(httpRequest.getBodyType())) {
            FormBody.Builder formBuilder = new FormBody.Builder();
            Map<String, String> multiparts = null;
            for (Map.Entry<String, String> entry : multiparts.entrySet()) {
                formBuilder.add(entry.getKey(), entry.getValue());
            }
            requestBody = formBuilder.build();
        } else {
            //TODO handle multipart
            //HttpRequest.BodyType = MULTIPART
            List<byte[]> bodyAsMultipart = httpRequest.getBodyAsMultipart();
        }
        builder.method(httpRequest.getMethod(),requestBody);
        return builder.build();
    }

    @Override
    public HttpResponse buildResponse(Response response) {
        HttpResponse httpResponse;
        try {
            LOGGER.debug("protocol: '{}'", response.protocol());
            LOGGER.debug("cache-control: '{}'", response.cacheControl());
            LOGGER.debug("handshake: '{}'", response.handshake());
            LOGGER.debug("challenges: '{}'", response.challenges());
            httpResponse = new HttpResponse(response.code(), response.message(), response.body()!=null?response.body().string():"");
            Headers headers = response.headers();
            Iterator<Pair<String, String>> iterator = headers.iterator();
            Map<String,List<String>> responseHeaders = Maps.newHashMap();
            while (iterator.hasNext()) {
                Pair<String, String> header = iterator.next();
                responseHeaders.put(header.getFirst(), Lists.newArrayList(header.getSecond()));
            }
            httpResponse.setResponseHeaders(responseHeaders);
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
