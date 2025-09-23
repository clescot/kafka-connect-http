package io.github.clescot.kafka.connect.http.client.okhttp;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.AbstractHttpClient;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.client.TimingData;
import io.github.clescot.kafka.connect.http.core.BodyType;
import io.github.clescot.kafka.connect.http.core.HttpPart;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
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
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.CookieStore;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.core.MediaType.APPLICATION_OCTET_STREAM;

public class OkHttpClient extends AbstractHttpClient<Request, Response> {


    private final okhttp3.OkHttpClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(OkHttpClient.class);
    private final Map<String, OkHttpClient> clients = Maps.newHashMap();


    public OkHttpClient(Map<String, String> config,
                        okhttp3.OkHttpClient client,
                        Random random) {
        super(config, random);
        Preconditions.checkNotNull(client, "okhttp3.OkHttpClient must not be null");
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
        //method
        String method = httpRequest.getMethod().name();
        RequestBody requestBody = getRequestBody(httpRequest, method, firstContentType);
        builder.method(method, requestBody);

        //timing data
        builder.tag(TimingData.class, new TimingData());
        return builder.build();
    }

    @Override
    public HttpRequest buildRequest(Request nativeRequest) {
        HttpRequest request = new HttpRequest(nativeRequest.url().toString(), HttpRequest.Method.valueOf(nativeRequest.method()), nativeRequest.headers().toMultimap(), BodyType.STRING);
        if (nativeRequest.body() != null) {
            final Buffer buffer = new Buffer();
            try {
                nativeRequest.body().writeTo(buffer);
                request.setBodyAsString(buffer.readUtf8());
            } catch (IOException e) {
                throw new HttpException(e);
            }
        }
        return request;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        OkHttpClient that = (OkHttpClient) o;
        return Objects.equals(client, that.client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), client);
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
        //BodyType = MULTIPART
        Map<String, HttpPart> bodyAsMultipart = httpRequest.getParts();
        String boundary = null;
        if (firstContentType != null && firstContentType.contains("boundary=")) {
            List<String> myParts = Lists.newArrayList(firstContentType.split("boundary="));
            if (myParts.size() == 2) {
                boundary = myParts.get(1);
            }
        }
        MultipartBody.Builder multipartBuilder = new MultipartBody.Builder(MoreObjects.firstNonNull(boundary, MoreObjects.firstNonNull(boundary, "---")));
        multipartBuilder.setType(MediaType.parse(io.github.clescot.kafka.connect.http.core.MediaType.MULTIPART_FORM_DATA));
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
                .filter(entry -> !entry.getKey().equalsIgnoreCase(io.github.clescot.kafka.connect.http.core.MediaType.KEY))
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


    private Map<String, String> fromNativeBodyToForm(ResponseBody responseBody) {
        Map<String, String> form = Maps.newHashMap();
        try {
            if (responseBody != null) {
                String bodyString = responseBody.string();
                String[] pairs = bodyString.split("&");
                for (String pair : pairs) {
                    String[] keyValue = pair.split("=");
                    if (keyValue.length == 2) {
                        form.put(keyValue[0], keyValue[1]);
                    }
                }
            }
        } catch (IOException e) {
            throw new HttpException(e);
        }
        return form;
    }

    @Override
    public HttpResponse buildResponse(Response response) {
        HttpResponse httpResponse = new HttpResponse(response.code(), response.message(), getStatusMessageLimit(), getHeadersLimit(), getBodyLimit());
        try {
            Protocol protocol = response.protocol();
            Handshake handshake = response.handshake();

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("native response :'{}'", response);
                LOGGER.trace("protocol: '{}',cache-control: '{}',handshake: '{}',challenges: '{}'", protocol, response.cacheControl(), handshake, response.challenges());
            }

            String contentType = response.header(io.github.clescot.kafka.connect.http.core.MediaType.KEY);
            if (contentType != null && !contentType.isEmpty()) {
                httpResponse.setContentType(contentType);
            } else {
                //default content type
                httpResponse.setContentType(io.github.clescot.kafka.connect.http.core.MediaType.APPLICATION_JSON);
            }
            ResponseBody body = response.body();
            switch (httpResponse.getBodyType()) {
                case BYTE_ARRAY -> httpResponse.setBodyAsByteArray(body.bytes());
                case FORM -> httpResponse.setBodyAsForm(fromNativeBodyToForm(body));
                case MULTIPART -> httpResponse.setParts(fromResponseBodyToParts(body));
                case STRING -> httpResponse.setBodyAsString(body.string());
            }


            if (protocol != null) {
                httpResponse.setProtocol(protocol.name());
            }
            Headers headers = response.headers();
            Iterator<Pair<String, String>> iterator = headers.iterator();
            Map<String, List<String>> responseHeaders = Maps.newHashMap();
            while (iterator.hasNext()) {
                Pair<String, String> header = iterator.next();
                responseHeaders.put(header.getFirst(), Lists.newArrayList(header.getSecond()));
            }
            httpResponse.setHeaders(responseHeaders);
            if(handshake!=null) {
                CipherSuite cipherSuite = handshake.cipherSuite();
                if(cipherSuite!=null) {
                   LOGGER.trace("cipher suite java name: {}", cipherSuite.javaName());
                }
                Principal localPrincipal = handshake.localPrincipal();
                if(localPrincipal !=null) {
                    String localPrincipalName = localPrincipal.getName();
                    LOGGER.trace("local principal: {}", localPrincipalName);
                }
                Principal peerPrincipal = handshake.peerPrincipal();
                if(peerPrincipal !=null) {
                    String peerPrincipalName = peerPrincipal.getName();
                    LOGGER.trace("peer principal: {}", peerPrincipalName);
                }
                List<Certificate> peeredCertificates = handshake.peerCertificates();
                if(peeredCertificates !=null) {
                    List<String> peerCertificates = peeredCertificates.stream().map(Object::toString).toList();
                    LOGGER.trace("peer certificates size:{}", peerCertificates.size());
                }
                List<Certificate> localedCertificates = handshake.localCertificates();
                if(localedCertificates !=null) {
                    List<String> localCertificates = localedCertificates.stream().map(Object::toString).toList();
                    LOGGER.trace("local certificates size:{}", localCertificates.size());
                }
                TlsVersion tlsVersion = handshake.tlsVersion();
                if(tlsVersion!=null){
                String tlsVersionName = tlsVersion.name();
                LOGGER.trace("tls version: {}", tlsVersionName);
                String tlsVersionJavaName = tlsVersion.javaName();
                LOGGER.trace("tls version java name: {}", tlsVersionJavaName);
                }
            }
        } catch (IOException e) {
            throw new HttpException(e);
        }
        return httpResponse;
    }

    private Map<String, HttpPart> fromResponseBodyToParts(ResponseBody body) {

        Map<String, HttpPart> parts = Maps.newHashMap();
        try {
            if (body != null) {
                MultipartReader multipartReader = new MultipartReader(body);
                MultipartReader.Part part;
                int inlinePartCount = 0;
                while ((part = multipartReader.nextPart()) != null) {
                    Headers headers = part.headers();
                    String contentDisposition = headers.get(HttpPart.CONTENT_DISPOSITION);
                    if (contentDisposition == null || contentDisposition.isBlank()) {
                        LOGGER.warn("Content-Disposition header is missing or empty for part. Skipping this part.");
                        continue;
                    }
                    String[] contentDispositionParts = contentDisposition.split(";");
                    String dispositionType = contentDispositionParts[0].trim();
                    Map<String, List<String>> headersMultimap = headers.toMultimap();
                    if(dispositionType.equalsIgnoreCase("inline")){

                        String partValue = part.body().readUtf8();
                        HttpPart httpPart = new HttpPart(headersMultimap,partValue);
                        inlinePartCount++;
                        parts.put("inline"+inlinePartCount, httpPart);
                    } else if (dispositionType.equalsIgnoreCase("attachment")) {
                        String fileName = contentDispositionParts[1].trim().replace("filename=", "").replace("\"", "");
                        HttpPart httpPart = new HttpPart(headersMultimap,part.body().readByteArray());
                        parts.put(fileName, httpPart);
                    }
                }
            }
        } catch (IOException e) {
            throw new HttpException(e);
        }
        return parts;
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

    public CookieJar getCookieJar() {
        return client.cookieJar();
    }

    /**
     * @return {@link okhttp3.OkHttpClient}
     */
    @Override
    public okhttp3.OkHttpClient getInternalClient() {
        return client;
    }

    @Override
    public Map<String, Long> getTimings(Request request, CompletableFuture<Response> response) {
        TimingData timingData = request.tag(TimingData.class);
        if (timingData != null) {
            Map<String,Long> timings = Maps.newHashMap();
            //convert ns to ms
            //if a timing is not available, its value will be 0
            //all timings are in ms
            timings.put("dns", timingData.getDnsDurationNs()/1000_000);//har:timings dns
            timings.put("connecting", timingData.getConnectingDurationNs()/1000_000);//har:timings connect = proxy + dns + secureConnecting(ssl)
            timings.put("connected", timingData.getConnectedDurationNs()/1000_000); //har:timings send(requestHeaders+requestBody) + wait + receive(responseHeaders+responseBody) = total - connecting
            timings.put("secureConnecting", timingData.getSecureConnectingDurationNs()/1000_000);//har:timings ssl
            timings.put("proxySelection", timingData.getProxySelectionDurationNs()/1000_000);//
            timings.put("requestHeaders", timingData.getRequestHeadersDurationNs()/1000_000);
            timings.put("requestBody", timingData.getRequestBodyDurationNs()/1000_000);
            timings.put("responseHeaders", timingData.getResponseHeadersDurationNs()/1000_000);
            timings.put("responseBody", timingData.getResponseBodyDurationNs()/1000_000);
            timings.put("total", timingData.getTotalDurationNs()/1000_000);
            return timings;
        }
        return Maps.newHashMap();
    }

    /**
     * customize the HttpClient for the user.
     * @param vuId
     * @return
     */
    @Override
    public HttpClient<Request, Response> customizeForUser(String vuId) {
        if(!clients.containsKey(vuId)){
            OkHttpClient okHttpClient = new OkHttpClient(getConfig(), customizeOkHttpClientForUser(vuId, client), random);
            clients.put(vuId,okHttpClient);
            return okHttpClient;
        }else{
            return clients.get(vuId);
        }
    }


    /**
     * customize the okhttp client for the user.
     * @param vuId
     * @param client
     * @return
     */
    private okhttp3.OkHttpClient customizeOkHttpClientForUser(String vuId,okhttp3.OkHttpClient client) {
        okhttp3.OkHttpClient.Builder builder = client.newBuilder();
        CookieStore cookieStore = null;//an internal InMemoryCookieStore() will be used if null
        CookiePolicy cookiePolicy = CookiePolicy.ACCEPT_ALL;
        CookieManager cookieManager = new CookieManager(cookieStore,cookiePolicy);
        CookieJar cookieJar = new OkHttpCookieJar(cookieManager);
        builder.cookieJar(cookieJar);
        //TODO handle option for no cookies, or method not called and reuse same Http Client ?
        //builder.cookieJar(CookieJar.NO_COOKIES);
        //we could customize the client for the user here
        return builder.build();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
