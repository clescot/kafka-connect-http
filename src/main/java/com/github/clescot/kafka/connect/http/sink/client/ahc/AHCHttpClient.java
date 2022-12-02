package com.github.clescot.kafka.connect.http.sink.client.ahc;

import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.github.clescot.kafka.connect.http.sink.client.HttpException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.asynchttpclient.*;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class AHCHttpClient implements HttpClient<Request, Response> {

    private final static Logger LOGGER = LoggerFactory.getLogger(AHCHttpClient.class);
    public static final String PROXY_PREFIX = "proxy-";
    public static final String REALM_PREFIX = "realm-";
    private static final String WS_PROXY_HOST = "host";
    private static final String WS_PROXY_PORT = "port";
    private static final String WS_PROXY_SECURED_PORT = "secured-port";
    private static final String WS_PROXY_NON_PROXY_HOSTS = "non-proxy-hosts";
    private static final String WS_PROXY_TYPE = "type";
    private static final String WS_REALM_PRINCIPAL_NAME = "principal-name";
    private static final String WS_REALM_ALGORITHM = "algorithm";
    private static final String WS_REALM_CHARSET = "charset";
    private static final String WS_REALM_LOGIN_CONTEXT_NAME = "login-context-name";
    private static final String WS_REALM_METHOD = "method";
    private static final String WS_REALM_NC = "nc";
    private static final String WS_REALM_NONCE = "nonce";
    private static final String WS_REALM_NTLM_DOMAIN = "ntlm-domain";
    private static final String WS_REALM_NTLM_HOST = "ntlm-host";
    private static final String WS_REALM_OMIT_QUERY = "omit-query";
    private static final String WS_REALM_OPAQUE = "opaque";
    private static final String WS_REALM_QOP = "qop";
    private static final String WS_REALM_SERVICE_PRINCIPAL_NAME = "service-principal-name";
    private static final String WS_REALM_NAME = "name";
    private static final String WS_REALM_USE_PREEMPTIVE_AUTH = "use-preemptive-auth";
    private static final String WS_REALM_AUTH_SCHEME = "auth-scheme";
    public static final String HTTP_PROXY_TYPE = "HTTP";
    public static final String UTF_8 = "UTF-8";

    public static final boolean FAILURE = false;

    public static final String WS_REQUEST_TIMEOUT_IN_MS = "request-timeout-in-ms";
    public static final String WS_READ_TIMEOUT_IN_MS = "read-timeout-in-ms";
    private static final String WS_REALM_PASS = "password";


    private final AsyncHttpClient asyncHttpClient;

    private final HttpClientAsyncCompletionHandler asyncCompletionHandler = new HttpClientAsyncCompletionHandler();

    public AHCHttpClient(AsyncHttpClient asyncHttpClient) {
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public org.asynchttpclient.Response nativeCall(org.asynchttpclient.Request request) {
        LOGGER.debug("native call  {}",request);
        ListenableFuture<Response> responseListenableFuture = asyncHttpClient.executeRequest(request, asyncCompletionHandler);
        //we cannot use the asynchronous nature of the response yet
        try {
            return responseListenableFuture.get();
        } catch (InterruptedException|ExecutionException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public org.asynchttpclient.Request buildRequest(HttpRequest httpRequest) {
        Preconditions.checkNotNull(httpRequest, "'httpRequest' is required but null");
        Preconditions.checkNotNull(httpRequest.getHeaders(), "'headers' are required but null");
        Preconditions.checkNotNull(httpRequest.getBodyAsString(), "'body' is required but null");
        String url = httpRequest.getUrl();
        Preconditions.checkNotNull(url, "'url' is required but null");
        String method = httpRequest.getMethod();
        Preconditions.checkNotNull(method, "'method' is required but null");

        //extract http headers
        Map<String, List<String>> httpHeaders = httpRequest.getHeaders();

        RequestBuilder requestBuilder = new RequestBuilder()
                .setUrl(url)
                .setHeaders(httpHeaders)
                .setMethod(method)
                .setBody(httpRequest.getBodyAsString());

        //extract proxy headers
        Map<String, String> proxyHeaders = Maps.newHashMap();
        httpHeaders.entrySet().stream().filter(entry -> entry.getKey().startsWith(PROXY_PREFIX))
                .forEach(entry -> proxyHeaders.put(entry.getKey().substring(PROXY_PREFIX.length()), entry.getValue().get(0)));

        defineProxyServer(requestBuilder, proxyHeaders);

        //extract proxy headers
        Map<String, String> realmHeaders = Maps.newHashMap();
        httpHeaders.entrySet().stream().filter(entry -> entry.getKey().startsWith(PROXY_PREFIX))
                .forEach(entry -> realmHeaders.put(entry.getKey().substring(REALM_PREFIX.length()), entry.getValue().get(0)));

        defineRealm(requestBuilder, proxyHeaders);
        //retry stuff
        int requestTimeoutInMillis;
        if (httpHeaders.get(WS_REQUEST_TIMEOUT_IN_MS) != null) {
            requestTimeoutInMillis = Integer.parseInt(httpHeaders.get(WS_REQUEST_TIMEOUT_IN_MS).get(0));
            requestBuilder.setRequestTimeout(requestTimeoutInMillis);
        }

        int readTimeoutInMillis;
        if (httpHeaders.get(WS_READ_TIMEOUT_IN_MS) != null) {
            readTimeoutInMillis = Integer.parseInt(httpHeaders.get(WS_READ_TIMEOUT_IN_MS).get(0));
            requestBuilder.setReadTimeout(readTimeoutInMillis);
        }
        return requestBuilder.build();
    }



    private void defineProxyServer(RequestBuilder requestBuilder, Map<String, String> proxyHeaders) {
        //proxy stuff
        String proxyHost = proxyHeaders.get(WS_PROXY_HOST);
        if (proxyHost != null) {
            int proxyPort = Integer.parseInt(proxyHeaders.get(WS_PROXY_PORT));
            ProxyServer.Builder proxyBuilder = new ProxyServer.Builder(proxyHost, proxyPort);

            String securedPortAsString = proxyHeaders.get(WS_PROXY_SECURED_PORT);
            if (securedPortAsString != null) {
                int securedProxyPort = Integer.parseInt(securedPortAsString);
                proxyBuilder.setSecuredPort(securedProxyPort);
            }

            String nonProxyHosts = proxyHeaders.get(WS_PROXY_NON_PROXY_HOSTS);

            if (isNotNullOrEmpty(nonProxyHosts)) {
                List<String> nonProxyHostAsList = Lists.newArrayList(nonProxyHosts.split(","));
                proxyBuilder.setNonProxyHosts(nonProxyHostAsList);
            }
            proxyBuilder.setProxyType(ProxyType.valueOf(Optional.ofNullable(proxyHeaders.get(WS_PROXY_TYPE)).orElse(HTTP_PROXY_TYPE)));

            requestBuilder.setProxyServer(proxyBuilder.build());
        }
    }

    private void defineRealm(RequestBuilder requestBuilder, Map<String, String> realmHeaders) {
        String principals = realmHeaders.get(WS_REALM_PRINCIPAL_NAME);
        String passwords = realmHeaders.get(WS_REALM_PASS);
        if (isNotNullOrEmpty(principals)
                && isNotNullOrEmpty(passwords)) {
            Realm.Builder realmBuilder = new Realm.Builder(principals, passwords);

            realmBuilder.setAlgorithm(realmHeaders.get(WS_REALM_ALGORITHM));

            realmBuilder.setCharset(Charset.forName(Optional.ofNullable(realmHeaders.get(WS_REALM_CHARSET)).orElse(UTF_8)));

            realmBuilder.setLoginContextName(realmHeaders.get(WS_REALM_LOGIN_CONTEXT_NAME));

            String authSchemeAsString = realmHeaders.get(WS_REALM_AUTH_SCHEME);
            Realm.AuthScheme authScheme = authSchemeAsString != null ? Realm.AuthScheme.valueOf(authSchemeAsString) : null;

            realmBuilder.setScheme(authScheme);

            realmBuilder.setMethodName(Optional.ofNullable(realmHeaders.get(WS_REALM_METHOD)).orElse("GET"));

            realmBuilder.setNc(realmHeaders.get(WS_REALM_NC));

            realmBuilder.setNonce((realmHeaders.get(WS_REALM_NONCE)));

            realmBuilder.setNtlmDomain(Optional.ofNullable(realmHeaders.get(WS_REALM_NTLM_DOMAIN)).orElse(System.getProperty("http.auth.ntlm.domain")));

            realmBuilder.setNtlmHost(Optional.ofNullable(realmHeaders.get(WS_REALM_NTLM_HOST)).orElse("localhost"));

            realmBuilder.setOmitQuery(Boolean.parseBoolean(realmHeaders.get(WS_REALM_OMIT_QUERY)));

            realmBuilder.setOpaque(realmHeaders.get(WS_REALM_OPAQUE));

            realmBuilder.setQop(realmHeaders.get(WS_REALM_QOP));

            realmBuilder.setServicePrincipalName(realmHeaders.get(WS_REALM_SERVICE_PRINCIPAL_NAME));

            realmBuilder.setRealmName(realmHeaders.get(WS_REALM_NAME));

            realmBuilder.setUsePreemptiveAuth(Boolean.parseBoolean(realmHeaders.get(WS_REALM_USE_PREEMPTIVE_AUTH)));

            requestBuilder.setRealm(realmBuilder.build());
        }
    }

    private boolean isNotNullOrEmpty(String field) {
        return field != null && !field.isEmpty();
    }
    public HttpResponse buildResponse(Response response) throws HttpException {
        List<Map.Entry<String, String>> responseEntries = response.getHeaders() != null ? response.getHeaders().entries() : Lists.newArrayList();
        HttpResponse httpResponse = new HttpResponse(response.getStatusCode(), response.getStatusText(), response.getResponseBody());
        Map<String, List<String>> responseHeaders = responseEntries.stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<String, List<String>>(entry.getKey(), Lists.newArrayList(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        httpResponse.setResponseHeaders(responseHeaders);
        return httpResponse;
    }


}
