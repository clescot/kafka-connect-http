package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.client.AbstractHttpClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kotlin.Pair;
import okhttp3.*;
import okhttp3.internal.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class OkHttpClient extends AbstractHttpClient<Request, Response> {
    private static final String PROTOCOL_SEPARATOR = ",";
    private okhttp3.OkHttpClient client;
    private Logger LOGGER = LoggerFactory.getLogger(OkHttpClient.class);

    public OkHttpClient(Map<String, String> config) {
        super(config);
        okhttp3.OkHttpClient.Builder httpClientBuilder = new okhttp3.OkHttpClient.Builder();
        //protocols
        if(config.containsKey(HTTPCLIENT_DEFAULT_PROTOCOLS)) {
            String protocolNames = config.get(HTTPCLIENT_DEFAULT_PROTOCOLS);
            List<Protocol> protocols = Lists.newArrayList();
            List<String> strings = Lists.newArrayList(protocolNames.split(PROTOCOL_SEPARATOR));
            for (String protocolName : strings) {
                Protocol protocol = Protocol.valueOf(protocolName);
                protocols.add(protocol);
            }
            httpClientBuilder.protocols(protocols);
        }
        //KeyManager/trustManager/SSLSocketFactory
        Optional<KeyManagerFactory> keyManagerFactoryOption = getKeyManagerFactory();
        Optional<TrustManagerFactory> trustManagerFactoryOption = getTrustManagerFactory();
        if(keyManagerFactoryOption.isPresent()||trustManagerFactoryOption.isPresent()) {
            SSLSocketFactory ssl = AbstractHttpClient.getSSLSocketFactory(keyManagerFactoryOption.orElse(null), trustManagerFactoryOption.orElse(null), "SSL");
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

        //call timeout
        if(config.containsKey(HTTPCLIENT_DEFAULT_CALL_TIMEOUT)) {
            int callTimeout = Integer.parseInt(config.get(HTTPCLIENT_DEFAULT_CALL_TIMEOUT));
            httpClientBuilder.callTimeout(callTimeout, TimeUnit.MILLISECONDS);
        }

        //connect timeout
        if(config.containsKey(HTTPCLIENT_DEFAULT_CONNECT_TIMEOUT)) {
            int connectTimeout = Integer.parseInt(config.get(HTTPCLIENT_DEFAULT_CONNECT_TIMEOUT_DOC));
            httpClientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
        }

        //read timeout
        if(config.containsKey(HTTPCLIENT_DEFAULT_READ_TIMEOUT)) {
            int readTimeout = Integer.parseInt(config.get(HTTPCLIENT_DEFAULT_READ_TIMEOUT_DOC));
            httpClientBuilder.readTimeout(readTimeout, TimeUnit.MILLISECONDS);
        }

        //write timeout
        if(config.containsKey(HTTPCLIENT_DEFAULT_WRITE_TIMEOUT)) {
            int writeTimeout = Integer.parseInt(config.get(HTTPCLIENT_DEFAULT_WRITE_TIMEOUT_DOC));
            httpClientBuilder.writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);
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
        String method = httpRequest.getMethod();
        if(HttpMethod.permitsRequestBody(method)) {
            if (HttpRequest.BodyType.STRING.equals(httpRequest.getBodyType())) {
                //use the contentType set in HttpRequest. if not set, use application/json
                requestBody = RequestBody.create(httpRequest.getBodyAsString(), MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/json")));
            } else if (HttpRequest.BodyType.BYTE_ARRAY.equals(httpRequest.getBodyType())) {
                String encoded = Base64.getEncoder().encodeToString(httpRequest.getBodyAsByteArray());
                requestBody = RequestBody.create(encoded, MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/octet-stream")));
            } else if (HttpRequest.BodyType.FORM.equals(httpRequest.getBodyType())) {
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
        }else if(httpRequest.getBodyAsString()!=null && !httpRequest.getBodyAsString().isBlank()){
            LOGGER.warn("Http Request with '{}' method does not permit a body. the provided body has been removed. please use another method to use one",method);
        }
        builder.method(method,requestBody);
        return builder.build();
    }

    @Override
    public HttpResponse buildResponse(Response response) {
        HttpResponse httpResponse;
        try {
            Protocol protocol = response.protocol();
            LOGGER.debug("protocol: '{}'", protocol);
            LOGGER.debug("cache-control: '{}'", response.cacheControl());
            LOGGER.debug("handshake: '{}'", response.handshake());
            LOGGER.debug("challenges: '{}'", response.challenges());
            httpResponse = new HttpResponse(response.code(), response.message());
            if(response.body()!=null) {
                httpResponse.setResponseBody(response.body().string());
            }
            if(protocol!=null) {
                httpResponse.setProtocol(protocol.name());
            }
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
    public CompletableFuture<Response> nativeCall(Request request) {
        CompletableFuture<Response> cf = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure( Call call, IOException e) {
                cf.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call,Response response) {
                cf.complete(response);
            }
        });
                return cf;
    }

}
