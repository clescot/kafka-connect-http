package io.github.clescot.kafka.connect.http.client.okhttp;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.AbstractHttpClient;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.core.HttpPart;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.HttpResponseBuilder;
import kotlin.Pair;
import okhttp3.*;
import okhttp3.internal.http.HttpMethod;
import okio.Buffer;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.core.ContentType.APPLICATION_OCTET_STREAM;

public class OkHttpClient extends AbstractHttpClient<Request, Response> {


    private final okhttp3.OkHttpClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(OkHttpClient.class);


    public OkHttpClient(Map<String, Object> config,
                        okhttp3.OkHttpClient client) {
        super(config);
        this.client = client;
    }

    @Override
    public Request buildNativeRequest(HttpRequest httpRequest) {
        Request.Builder builder = new Request.Builder();

        //url
        String url = httpRequest.getUrl();
        HttpUrl okHttpUrl = HttpUrl.parse(url);
        Preconditions.checkNotNull(okHttpUrl, "url cannot be null");
        builder.url(okHttpUrl);

        //headers
        Map<String, List<String>> headers = httpRequest.getHeaders();
        Headers okHeaders = getHeaders(headers);
        builder.headers(okHeaders);
        //Content-Type
        List<String> contentType = headers.get("Content-Type");
        String firstContentType = null;
        if (contentType != null && !contentType.isEmpty()) {
            firstContentType = contentType.get(0);
        }
        String method = httpRequest.getMethod().name();
        RequestBody requestBody = getRequestBody(httpRequest, method, firstContentType);
        builder.method(method, requestBody);
        return builder.build();
    }

    @Override
    public HttpRequest buildRequest(Request nativeRequest) {
        HttpRequest request = new HttpRequest(nativeRequest.url().toString(), HttpRequest.Method.valueOf(nativeRequest.method()), nativeRequest.headers().toMultimap(), HttpRequest.BodyType.STRING);
        if (nativeRequest.body() != null) {
            final Buffer buffer = new Buffer();
            try {
                nativeRequest.body().writeTo(buffer);
                request.setBodyAsString(buffer.readUtf8());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return request;
    }

    @Nullable
    private RequestBody getRequestBody(HttpRequest httpRequest, String method, String firstContentType) {
        RequestBody requestBody = null;
        if (HttpMethod.permitsRequestBody(method)) {
            switch (httpRequest.getBodyType()) {

                case BYTE_ARRAY: {
                    byte[] bodyAsByteArray = httpRequest.getBodyAsByteArray();
                    requestBody = toRequestBody(bodyAsByteArray, firstContentType);
                    //file upload through a FORM is the same as a byte array (content of the file). this case handle also this case, except part of a multipart request
                    break;
                }

                case FORM: {
                    //contentType is  application/x-www-form-urlencoded

                    Map<String, String> entries = httpRequest.getBodyAsForm();
                    FormBody.Builder formBuilder = new FormBody.Builder();
                    for (Map.Entry<String, String> entry : entries.entrySet()) {
                        formBuilder.add(entry.getKey(), entry.getValue());
                    }
                    requestBody = formBuilder.build();
                    break;
                }
                case MULTIPART: {
                    requestBody = getMultiPartRequestBody(httpRequest, firstContentType);
                    break;
                }

                case STRING:
                default: {
                    //use the contentType set in HttpRequest. if not set, use application/json
                    requestBody = RequestBody.create(httpRequest.getBodyAsString(), MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/json")));
                    break;
                }
            }

        } else if (httpRequest.getBodyAsString() != null && !httpRequest.getBodyAsString().isBlank()) {
            LOGGER.warn("Http Request with '{}' method does not permit a body. the provided body has been removed. please use another method to use one", method);
        }
        return requestBody;
    }

    @NotNull
    private RequestBody getMultiPartRequestBody(HttpRequest httpRequest, String firstContentType) {
        RequestBody requestBody;
        //HttpRequest.BodyType = MULTIPART
        Map<String, HttpPart> bodyAsMultipart = httpRequest.getParts();
        String boundary = null;
        if (firstContentType != null && firstContentType.contains("boundary=")) {
            List<String> myParts = Lists.newArrayList(firstContentType.split("boundary="));
            if (myParts.size() == 2) {
                boundary = myParts.get(1);
            }
        }
        MultipartBody.Builder multipartBuilder = new MultipartBody.Builder(MoreObjects.firstNonNull(boundary, MoreObjects.firstNonNull(boundary, "---")));
        multipartBuilder.setType(MediaType.parse("multipart/form-data"));
        for (Map.Entry<String, HttpPart> entry : bodyAsMultipart.entrySet()) {
            RequestBody partRequestBody;
            String parameterName = entry.getKey();
            HttpPart httpPart = entry.getValue();
            Map<String, List<String>> partHeaders = httpPart.getHeaders();
            Headers okPartHeaders = getHeaders(filterHeaders(partHeaders));
            switch (httpPart.getBodyType()) {
                //HttpPart is <string,optional<file>>
                case FORM_DATA:
                    Map.Entry<String, File> contentAsFormEntry = httpPart.getContentAsFormEntry();
                    String fileName = contentAsFormEntry.getKey();
                    File file = contentAsFormEntry.getValue();
                    requestBody = RequestBody.create(file, MediaType.parse(APPLICATION_OCTET_STREAM));
                    multipartBuilder.addFormDataPart(parameterName, fileName, requestBody);
                    break;
                //HttpPart is <fileUri>
                case FORM_DATA_AS_REFERENCE:
                    Map.Entry<String, File> formEntry = httpPart.getContentAsFormEntry();
                    File fileAsReference = new File(httpPart.getFileUri());
                    requestBody = RequestBody.create(fileAsReference, MediaType.parse(APPLICATION_OCTET_STREAM));
                    multipartBuilder.addFormDataPart(parameterName, formEntry.getKey(), requestBody);
                    break;
                //HttpPart is <string,byte[]>
                case BYTE_ARRAY:
                    partRequestBody = toRequestBody(httpPart.getContentAsByteArray(), httpPart.getContentType());
                    multipartBuilder.addPart(okPartHeaders, partRequestBody);
                    break;
                //HttpPart is <string>
                case STRING:
                default:
                    String contentAsString = httpPart.getContentAsString();
                    partRequestBody = RequestBody.create(contentAsString, MediaType.parse(httpPart.getContentType()));
                    multipartBuilder.addPart(okPartHeaders, partRequestBody);
                    break;
            }
        }
        requestBody = multipartBuilder.build();
        return requestBody;
    }

    private Map<String, List<String>> filterHeaders(Map<String, List<String>> headers) {
        return headers != null ? headers.entrySet().stream()
                .filter(entry -> !entry.getKey().equalsIgnoreCase("Content-Type"))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )
                ) : Maps.newHashMap();
    }


    @NotNull
    private static Headers getHeaders(Map<String, List<String>> headers) {
        Headers.Builder okHeadersBuilder = new Headers.Builder();
        headers.forEach((key, values) -> {
            for (String value : values) {
                okHeadersBuilder.add(key, value);
            }
        });
        return okHeadersBuilder.build();
    }

    @NotNull
    private static RequestBody toRequestBody(byte[] bodyAsByteArray, String contentType) {
        RequestBody requestBody;
        String encoded = Base64.getEncoder().encodeToString(bodyAsByteArray);
        //use the contentType set in HttpRequest. if not set, use application/octet-stream
        requestBody = RequestBody.create(encoded, MediaType.parse(Optional.ofNullable(contentType).orElse(APPLICATION_OCTET_STREAM)));
        return requestBody;
    }


    @Override
    public HttpResponse buildResponse(HttpResponseBuilder httpResponseBuilder, Response response) {
        try {
            Protocol protocol = response.protocol();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("native response :'{}'", response);
                LOGGER.trace("protocol: '{}',cache-control: '{}',handshake: '{}',challenges: '{}'", protocol, response.cacheControl(), response.handshake(), response.challenges());
            }
            httpResponseBuilder.setStatus(response.code(), response.message());
            // handle more bodyType for HttpResponse :
            //TODO https://github.com/clescot/kafka-connect-http/issues/784
            //TODO https://github.com/clescot/kafka-connect-http/issues/785
            //TODO https://github.com/clescot/kafka-connect-http/issues/786
            if (response.body() != null) {
                httpResponseBuilder.setBodyAsString(response.body().string());
            }
            if (protocol != null) {
                httpResponseBuilder.setProtocol(protocol.name());
            }
            Headers headers = response.headers();
            Iterator<Pair<String, String>> iterator = headers.iterator();
            Map<String, List<String>> responseHeaders = Maps.newHashMap();
            while (iterator.hasNext()) {
                Pair<String, String> header = iterator.next();
                responseHeaders.put(header.getFirst(), Lists.newArrayList(header.getSecond()));
            }
            httpResponseBuilder.setHeaders(responseHeaders);
        } catch (IOException e) {
            throw new HttpException(e);
        }
        return httpResponseBuilder.toHttpResponse();
    }

    @Override
    public CompletableFuture<Response> nativeCall(Request request) {
        CompletableFuture<Response> cf = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                LOGGER.error("okhttp native call error :{}", ExceptionUtils.getStackTrace(e));
                cf.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) {
                cf.complete(response);
            }
        });
        return cf;
    }

    @Override
    public String getEngineId() {
        return "okhttp";
    }

    /**
     * @return {@link okhttp3.OkHttpClient}
     */
    @Override
    public okhttp3.OkHttpClient getInternalClient() {
        return client;
    }
}
