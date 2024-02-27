package io.github.clescot.kafka.connect.http.client.oauth;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.github.scribejava.httpclient.okhttp.OkHttpHttpClient;
import com.google.common.base.Joiner;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.client.oauth.MockOAuth2ServerInitializer.MOCK_OAUTH_2_SERVER_BASE_URL;
import static io.github.clescot.kafka.connect.http.client.oauth.OAuth2AuthorizationCodeLoginAppTest.PROVIDER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = OAuth2LoginApp.class,
        //these can be set in application yaml if you desire
        properties = {
                OAuth2AuthorizationCodeLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-id=testclient",
                OAuth2AuthorizationCodeLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-secret=testsecret",
                OAuth2AuthorizationCodeLoginAppTest.REGISTRATION + PROVIDER_ID + ".authorization-grant-type=authorization_code",
                OAuth2AuthorizationCodeLoginAppTest.REGISTRATION + PROVIDER_ID + ".redirect-uri={baseUrl}/login/oauth2/code/{registrationId}",
                OAuth2AuthorizationCodeLoginAppTest.REGISTRATION + PROVIDER_ID + ".scope=openid",
                OAuth2AuthorizationCodeLoginAppTest.PROVIDER + PROVIDER_ID + ".authorization-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/authorize",
                OAuth2AuthorizationCodeLoginAppTest.PROVIDER + PROVIDER_ID + ".token-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/token",
                OAuth2AuthorizationCodeLoginAppTest.PROVIDER + PROVIDER_ID + ".jwk-set-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/jwks"
        }
)
@ContextConfiguration(initializers = {MockOAuth2ServerInitializer.class})
public class OAuth2AuthorizationCodeLoginAppTest {
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
    public void oidc_login_with_okhttp() throws IOException, ExecutionException, InterruptedException {

        mockOAuth2Server.enqueueCallback(new DefaultOAuth2TokenCallback("issuer1", "foo"));
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .followRedirects(true).build();
        OAuth20Service oAuth20Service = new ServiceBuilder("testclient")
                .apiSecret("testsecret")
                .debug()
                .httpClient(new OkHttpHttpClient(okHttpClient)).build(new DefaultApi20() {
                    @Override
                    public String getAccessTokenEndpoint() {
                        return  "http://localhost:" + localPort+"/issuer1/token";
                    }

                    @Override
                    protected String getAuthorizationBaseUrl() {
                        return "http://localhost:" + localPort;
                    }
                });
        OAuth2AccessToken accessToken = oAuth20Service.getAccessToken("");

    }

    private ClientHttpConnector followRedirectsWithCookies(Map<String, Cookie> cookieManager) {
        return new ReactorClientHttpConnector(
                HttpClient
                        .create()
                        .doOnRequest((req,conn)-> {
                            System.out.println("---> request: "+req.method()+" "+req.resourceUrl());
                            System.out.println("request headers:\n"+Joiner.on("\n").join(
                                    req.requestHeaders()
                                            .entries()
                                            .stream()
                                            .map((e)-> "- "+e.getKey()+"="+e.getValue()).collect(Collectors.toSet())));
                            System.out.println("headers end");
                        })
                        .doOnResponse((res,conn)-> System.out.println("<--- response:"+res))
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