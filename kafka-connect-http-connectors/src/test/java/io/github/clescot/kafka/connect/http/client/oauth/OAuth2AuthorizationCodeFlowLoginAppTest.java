package io.github.clescot.kafka.connect.http.client.oauth;

import com.google.common.base.Joiner;
import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.rar.AuthorizationDetail;
import com.nimbusds.oauth2.sdk.token.*;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.github.clescot.kafka.connect.http.client.okhttp.interceptor.LoggingInterceptor;
import io.netty.handler.codec.http.cookie.Cookie;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.client.oauth.MockOAuth2ServerInitializer.MOCK_OAUTH_2_SERVER_BASE_URL;
import static io.github.clescot.kafka.connect.http.client.oauth.OAuth2AuthorizationCodeFlowLoginAppTest.PROVIDER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Authorization Code Flow is used to impersonate the end user.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = OAuth2LoginApp.class,
        //these can be set in application yaml if you desire
        properties = {
                OAuth2AuthorizationCodeFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-id=testclient",
                OAuth2AuthorizationCodeFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-secret=testsecret",
                OAuth2AuthorizationCodeFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".authorization-grant-type=authorization_code",
                OAuth2AuthorizationCodeFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".redirect-uri={baseUrl}/login/oauth2/code/{registrationId}",
                OAuth2AuthorizationCodeFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".scope=openid",
                OAuth2AuthorizationCodeFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".authorization-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/authorize",
                OAuth2AuthorizationCodeFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".token-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/token",
                OAuth2AuthorizationCodeFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".jwk-set-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/jwks"
        }
)
@ContextConfiguration(initializers = {MockOAuth2ServerInitializer.class})
public class OAuth2AuthorizationCodeFlowLoginAppTest {
    public static final String CLIENT = "spring.security.oauth2.client";
    public static final String PROVIDER = CLIENT + ".provider.";
    public static final String REGISTRATION = CLIENT + ".registration.";
    public static final String PROVIDER_ID = "myprovider";

    @LocalServerPort
    private int localPort;

    @Autowired
    private MockOAuth2Server mockOAuth2Server;

    @Test
    public void oidcUserFooShouldBeLoggedIn() {
        System.out.println("local port is " + localPort);
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
    public void oidc_login_with_okhttp() throws IOException, URISyntaxException, ParseException {

        //get oidc provider metadata
        String wellKnownurl = mockOAuth2Server.wellKnownUrl("default").toString();
        mockOAuth2Server.enqueueCallback(new DefaultOAuth2TokenCallback("issuer1", "foo"));

        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .followRedirects(true)
                .addNetworkInterceptor(new LoggingInterceptor())
                .build();


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
        System.out.println("issuer="+issuer);
        System.out.println(providerMetadata);

        //get access token with client credential flow

        // Construct the client credentials grant
        AuthorizationGrant clientGrant = new ClientCredentialsGrant();

        // The credentials to authenticate the client at the token endpoint
        ClientID clientID = new ClientID("testclient");
        Secret clientSecret = new Secret("testsecret");
        ClientAuthentication clientAuth = new ClientSecretBasic(clientID, clientSecret);

        // The request scope for the token (may be optional)
        Scope scope = new Scope("openid");
//        Scope scope = new Scope("read", "write");

        // The token endpoint

        // Make the token request
        TokenRequest request = new TokenRequest(tokenEndpointUri, clientAuth, clientGrant, scope);

        TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

        if (!response.indicatesSuccess()) {
            // We got an error response...
            TokenErrorResponse errorResponse = response.toErrorResponse();
            System.out.println(errorResponse.toJSONObject());
        }

        AccessTokenResponse successResponse = response.toSuccessResponse();

        // Get the access token
        Tokens tokens = successResponse.getTokens();
        AccessToken accessToken = tokens.getAccessToken();
        AccessTokenType type = accessToken.getType();
        System.out.println("access token type:" + type.toString());
        TokenTypeURI issuedTokenType = accessToken.getIssuedTokenType();
        System.out.println("access token issuedTokenType:" + issuedTokenType);
        List<AuthorizationDetail> authorizationDetails = accessToken.getAuthorizationDetails();
        if(authorizationDetails!=null) {
            System.out.println("access token authorizationDetails:" + Joiner.on(",").join(authorizationDetails));
        }
        Scope scope1 = accessToken.getScope();
        System.out.println("access token scope:" + scope1);
        long lifetime = accessToken.getLifetime();
        System.out.println("access token lifetime:" + lifetime);
        System.out.println("access token toString" + accessToken.toJSONString());
        Map<String, Object> customParameters = accessToken.getCustomParameters();
        System.out.println("access token customParameters" + Joiner.on(",").join(customParameters.entrySet()));
        RefreshToken refreshToken = tokens.getRefreshToken();
        System.out.println(refreshToken);


    }

    private ClientHttpConnector followRedirectsWithCookies(Map<String, Cookie> cookieManager) {
        return new ReactorClientHttpConnector(
                HttpClient
                        .create()
                        .doOnRequest((req, conn) -> {
                            System.out.println("---> request: " + req.method() + " " + req.resourceUrl());
                            System.out.println("request headers:\n" + Joiner.on("\n").join(
                                    req.requestHeaders()
                                            .entries()
                                            .stream()
                                            .map((e) -> "- " + e.getKey() + "=" + e.getValue()).collect(Collectors.toSet())));
                            System.out.println("headers end");
                        })
                        .doOnResponse((res, conn) -> System.out.println("<--- response:" + res))
                        .followRedirect((req, resp) -> {
                                    List<Map.Entry<String, String>> requestHeaders = req.requestHeaders().entries();
                                    List<Map.Entry<String, String>> responseHeaders = resp.responseHeaders().entries();
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