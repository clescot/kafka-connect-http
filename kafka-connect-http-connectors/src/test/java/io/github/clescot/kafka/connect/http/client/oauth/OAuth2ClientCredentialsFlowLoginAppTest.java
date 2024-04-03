package io.github.clescot.kafka.connect.http.client.oauth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.netty.handler.codec.http.cookie.Cookie;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.client.oauth.MockOAuth2ServerInitializer.MOCK_OAUTH_2_SERVER_BASE_URL;
import static io.github.clescot.kafka.connect.http.client.oauth.OAuth2AuthorizationCodeFlowLoginAppTest.PROVIDER_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * machine to machine scenario.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = OAuth2LoginApp.class,
        //these can be set in application yaml if you desire
        properties = {
                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-id=testclient",
                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-secret=testsecret",
                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".authorization-grant-type=client_credentials",
                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".redirect-uri={baseUrl}/login/oauth2/code/{registrationId}",
                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".scope=openid",
                OAuth2ClientCredentialsFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".authorization-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/authorize",
                OAuth2ClientCredentialsFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".token-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/token",
                OAuth2ClientCredentialsFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".jwk-set-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/jwks"
        }
)
//client-authentication-method: client_secret_jwt client_secret_basic client_secret_post private_key_jwt none
@ContextConfiguration(initializers = {MockOAuth2ServerInitializer.class})
public class OAuth2ClientCredentialsFlowLoginAppTest {
    public static final String CLIENT = "spring.security.oauth2.client";
    public static final String PROVIDER = CLIENT + ".provider.";
    public static final String REGISTRATION = CLIENT + ".registration.";
    public static final String PROVIDER_ID = "myprovider";
    public static final String OPENID = "openid";
    public static final String ISSUER_ID = "default";

    @LocalServerPort
    private int localPort;

    @Autowired
    private MockOAuth2Server mockOAuth2Server;

    @Test
    public void oidcUserFooShouldBeLoggedIn() {
        Map<String, Cookie> cookieManager = new HashMap<>();
        WebClient webClient = WebClient.builder()
                .clientConnector(followRedirectsWithCookies(cookieManager))
                .build();

        mockOAuth2Server.enqueueCallback(new DefaultOAuth2TokenCallback("issuer1", "foo"));

        String response = webClient
                .mutate()
                .baseUrl("http://localhost:" + localPort)
                .build()
                .get()
                .uri("/api/ping")
                .header("Accept", "text/html")
                .retrieve()
                .bodyToMono(String.class).block();

        assertEquals("hello foo", response);
    }


    @Test
    public void oidc_login_with_okhttp() throws IOException, ParseException {

        //configure mock oauth2 server
        mockOAuth2Server.enqueueCallback(new DefaultOAuth2TokenCallback("issuer1", "foo"));

        //get oidc provider metadata
        String wellKnownurl = mockOAuth2Server.wellKnownUrl(ISSUER_ID).toString();
        URL providerConfigurationURL = new URL(wellKnownurl);
        InputStream stream = providerConfigurationURL.openStream();
        // Read all data from URL
        String providerInfo;
        try (java.util.Scanner s = new java.util.Scanner(stream)) {
            providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
        }
        OIDCProviderMetadata providerMetadata = OIDCProviderMetadata.parse(providerInfo);
        // The token endpoint
        URI tokenEndpointUri = providerMetadata.getTokenEndpointURI();
        String tokenEndpoint = tokenEndpointUri.toString();
        String issuer = tokenEndpoint.substring(0, tokenEndpoint.length() - "/token".length());

        //get access token with client credential flow

        // Construct the client credentials grant
        AuthorizationGrant clientGrant = new ClientCredentialsGrant();

        // The credentials to authenticate the client at the token endpoint
        ClientID clientID = new ClientID("testclient");
        Secret clientSecret = new Secret("testsecret");
        ClientAuthentication clientAuth = new ClientSecretBasic(clientID, clientSecret);

        // The request scope for the token (may be optional)
        Scope scope = new Scope(OPENID, "refresh_token");
//        Scope scope = new Scope("read", "write");


        // Make the token request
        Tokens tokens = getTokens(tokenEndpointUri, clientAuth, clientGrant, scope);
        AccessToken accessToken = tokens.getAccessToken();

        //no refresh token is issued for client credentials flow
        //cf RFC6749 section 4.4.3 https://www.rfc-editor.org/rfc/rfc6749#section-4.4.3

        // Get the access token as JSON string
        String accessTokenJSONString = accessToken.toJSONString();
        System.out.println(accessTokenJSONString);
        assertThat(accessToken.getScope().toString()).isEqualTo("openid refresh_token");
        assertThat(accessToken.getLifetime()).isEqualTo(3599L);
        String bearerToken = accessToken.toAuthorizationHeader();
        String[] chunks = bearerToken.split("Bearer ")[1].split("\\.");
        Base64.Decoder decoder = Base64.getUrlDecoder();
        String header = new String(decoder.decode(chunks[0]));
        //header fields
        //typ : token type
        //alg : signing algorithm
        //kid: key id (id of the key used to sign the token)
        JSONAssert.assertEquals("jwt header is wrong", "" +
                "{\n" +
                "  \"kid\": \"default\",\n" +
                "  \"typ\": \"JWT\",\n" +
                "  \"alg\": \"RS256\"\n" +
                "}", header, true);
        String payload = new String(decoder.decode(chunks[1]));
//        System.out.println("payload:"+payload);
        /*payload fields
          sub : Subject
          nbf : Not Before
          azp : Authorized Parties
          iss : principal that issued the JWT
          exp : expiration time
          iat : issued at time
          jti : JWT ID
          tid : tenant identifier

         */
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode payloadJsonNode = objectMapper.readTree(payload);
        JsonNode sub = payloadJsonNode.get("sub");
        assertThat(sub.asText()).isEqualTo("testclient");
        long currentTimeMillis = System.currentTimeMillis();
        assertThat(Long.parseLong(payloadJsonNode.get("nbf").toPrettyString()) * 1000).isLessThanOrEqualTo(currentTimeMillis);
        assertThat(payloadJsonNode.get("azp").asText()).isEqualTo("testclient");
        assertThat(payloadJsonNode.get("iss").asText()).isEqualTo(issuer);
        assertThat(Long.parseLong(payloadJsonNode.get("exp").toPrettyString()) * 1000).isGreaterThanOrEqualTo(currentTimeMillis);
        assertThat(Long.parseLong(payloadJsonNode.get("iat").toPrettyString()) * 1000).isLessThanOrEqualTo(currentTimeMillis);
        assertThat(payloadJsonNode.get("tid").asText()).isEqualTo(ISSUER_ID);
        String jti = payloadJsonNode.get("jti").asText();


        // Get the refresh token
        RefreshToken refreshToken = tokens.getRefreshToken();

        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .followRedirects(true)
                .followSslRedirects(true)
                .addNetworkInterceptor(new Interceptor() {
                    @NotNull
                    @Override
                    public Response intercept(@NotNull Chain chain) throws IOException {
                        Request request = chain.request();

                        long t1 = System.nanoTime();
                        String log = String.format("Sending request %s on %s%n%s", request.url(), chain.connection(), request.headers());
                        System.out.println(log);

                        Response response = chain.proceed(request);

                        long t2 = System.nanoTime();
                        double elapsedTime = (t2 - t1) / 1e6d;
                        String log2 = String.format("Received response for %s on %s%n%s in %.1fms%n%s %s%n%s",
                                response.request().url(), chain.connection(), request.headers(), elapsedTime, response.code(), response.message(), response.headers());
                        System.out.println(log2);
                        return response;
                    }
                })
                .build();

        HttpUrl okHttpUrl = HttpUrl.parse("http://localhost:" + localPort + "/api/ping");
        okhttp3.Request request1 = new Request.Builder()
                .url(okHttpUrl)
                .header("Accept", "text/html")
                .header("Authorization", bearerToken)
                .method("GET", null).build();
        Response response1 = okHttpClient.newCall(request1).execute();
        assertThat(response1).isNotNull();
        String bodyString = response1.body().string();
        assertThat(bodyString).isEqualTo("");
    }

    private static Tokens getTokens(URI tokenEndpointUri, ClientAuthentication clientAuth, AuthorizationGrant clientGrant, Scope scope) throws ParseException, IOException {
        TokenRequest request = new TokenRequest(tokenEndpointUri, clientAuth, clientGrant, scope);

        TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

        if (!response.indicatesSuccess()) {
            // We got an error response...
            TokenErrorResponse errorResponse = response.toErrorResponse();
            System.err.println("error: " + errorResponse.toString());
        }

        AccessTokenResponse successResponse = response.toSuccessResponse();

        Tokens tokens = successResponse.getTokens();
        return tokens;
    }

    private ClientHttpConnector followRedirectsWithCookies(Map<String, Cookie> cookieManager) {
        return new ReactorClientHttpConnector(
                HttpClient
                        .create()
                        .doOnRequest((req, conn) -> {
                            System.out.println("---> request:" + req.resourceUrl());
                            System.out.println("request headers:\n" + Joiner.on("\n").join(
                                    req.requestHeaders()
                                            .entries()
                                            .stream()
                                            .map((e) -> "- " + e.getKey() + "=" + e.getValue()).collect(Collectors.toSet())));
                            System.out.println("headers end");
                        })
                        .doOnResponse((res, conn) -> System.out.println("<--- response:" + res))
                        .followRedirect((req, resp) -> {
                                    for (var entry : resp.cookies().entrySet()) {
                                        var cookie = entry.getValue().stream().findFirst().orElse(null);
                                        if (cookie != null && cookie.value() != null && !cookie.value().isBlank()) {
                                            cookieManager.put(entry.getKey().toString(), cookie);
                                        }
                                    }
                                    System.out.println(resp.status());
                                    System.out.println("<--- redirect response headers:");
                                    System.out.println(Joiner.on("\n- ").join(resp.responseHeaders()));
                                    return resp.responseHeaders().contains("Location");
                                },
                                req -> {
                                    for (var cookie : cookieManager.entrySet()) {
                                        req.header("Cookie", cookie.getKey() + "=" + cookie.getValue().value());
                                    }
                                }
                        )
        );
    }
}