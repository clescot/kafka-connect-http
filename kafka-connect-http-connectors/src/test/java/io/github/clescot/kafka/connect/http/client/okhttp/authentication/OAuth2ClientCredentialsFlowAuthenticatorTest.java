package io.github.clescot.kafka.connect.http.client.okhttp.authentication;


import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import okhttp3.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class OAuth2ClientCredentialsFlowAuthenticatorTest {
    public static final String CLIENT_ID = "44d34a4d05344c97837d463207805f8b";
    public static final String CLIENT_SECRET = "3fc0576720544ac293a3a5304e6c0fa8";
    public static final String WELL_KNOWN_OPENID_CONFIGURATION = "/.well-known/openid-configuration";
    public static final String BAD_WELL_KNOWN_OPENID_CONFIGURATION = "/bad/.well-known/openid-configuration";
    public static final String BAD_TOKEN_WELL_KNOWN_OPENID_CONFIGURATION = "/bad/token/.well-known/openid-configuration";
    public static final String SONG_PATH = "/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V";
    @RegisterExtension
    static WireMockExtension wmHttp;

    static {

        wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                                .dynamicPort()
                                .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                )
                .build();
    }

    public static final String WELL_KNOWN_OK = "WellKnownOk";
    public static final String WELL_KNOWN_KO = "WellKnownKo";
    public static final String WELL_KNOWN_BAD_TOKEN = "WellKnownBadToken";
    public static final String TOKEN_OK = "TokenOk";
    public static final String TOKEN_KO = "TokenKo";
    public static final String SONG_OK = "SongOk";
    private String httpBaseUrl;

    @BeforeEach
    void setup() throws IOException {
        String scenario = "test_successful_request_at_first_time";
        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        Path path = Paths.get("src/test/resources/oauth2/wellknownUrlContent.json");
        httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
        String content = Files.readString(path);
        String wellKnownUrlContent = content.replaceAll("baseUrl", httpBaseUrl);

        wireMock
                .register(WireMock.get(WELL_KNOWN_OPENID_CONFIGURATION).inScenario(scenario)
                        .whenScenarioStateIs(STARTED)
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)
                                .withStatusMessage("OK")
                                .withBody(wellKnownUrlContent)
                        ).willSetStateTo(WELL_KNOWN_OK)
                );



        wireMock
                .register(WireMock.get(BAD_WELL_KNOWN_OPENID_CONFIGURATION).inScenario(scenario)
                        .whenScenarioStateIs(STARTED)
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)
                                .withStatusMessage("OK")
                                .withBody("dummy content")
                        ).willSetStateTo(WELL_KNOWN_KO)
                );

        Path pathWithBadToken = Paths.get("src/test/resources/oauth2/wellknownUrlContent.json");
        String contentWithBadTokenUri = Files.readString(pathWithBadToken);
        String wellKnownUrlContentWithBadTokenUri = contentWithBadTokenUri.replaceAll("baseUrl", httpBaseUrl);


        wireMock
                .register(WireMock.get(BAD_TOKEN_WELL_KNOWN_OPENID_CONFIGURATION).inScenario(scenario)
                        .whenScenarioStateIs(STARTED)
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)
                                .withStatusMessage("OK")
                                .withBody(wellKnownUrlContentWithBadTokenUri)
                        ).willSetStateTo(WELL_KNOWN_BAD_TOKEN)
                );

        Path tokenPath = Paths.get("src/test/resources/oauth2/token.json");
        String tokenContent = Files.readString(tokenPath);
        wireMock
                .register(
                        WireMock.post("/api/token")
                                .withHeader("Content-Type",containing("application/x-www-form-urlencoded; charset=UTF-8"))
                                .withHeader("Authorization",containing("NDRkMzRhNGQwNTM0NGM5NzgzN2Q0NjMyMDc4MDVmOGI6M2ZjMDU3NjcyMDU0NGFjMjkzYTNhNTMwNGU2YzBmYTg="))
                                .inScenario(scenario)
                                .whenScenarioStateIs(WELL_KNOWN_OK)
                                .willReturn(WireMock.aResponse()
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withBody(tokenContent)
                                ).willSetStateTo(TOKEN_OK)
                );

        wireMock
                .register(
                        WireMock.post("/bad/api/token")
                                .withHeader("Content-Type",containing("application/x-www-form-urlencoded; charset=UTF-8"))
                                .withHeader("Authorization",containing("NDRkMzRhNGQwNTM0NGM5NzgzN2Q0NjMyMDc4MDVmOGI6M2ZjMDU3NjcyMDU0NGFjMjkzYTNhNTMwNGU2YzBmYTg="))
                                .inScenario(scenario)
                                .whenScenarioStateIs(WELL_KNOWN_OK)
                                .willReturn(WireMock.aResponse()
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withBody("bad content")
                                ).willSetStateTo(TOKEN_KO)
                );


        Path songPath = Paths.get("src/test/resources/oauth2/song.json");
        String songContent = Files.readString(songPath);
        wireMock
                .register(
                        WireMock.get(SONG_PATH)
                                .withHeader("Authorization",containing("Bearer BQDzs98uhifaGayk8H9tCTRozufhFmgV_HKMCnnDdMTdz1FcOo3sdj8OZJ_azo96LRdLI9_1uJOCXxbGZme11KCb6ZxTuCt8B5FxEeECb1kO_-UDuf8"))
                                .inScenario(scenario)
                                .whenScenarioStateIs(TOKEN_OK)
                                .willReturn(WireMock.aResponse()
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withBody(songContent)
                                ).willSetStateTo(SONG_OK)
                );
    }

    @Test
    void test_constructor_with_null_args() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                null, null, null, null));
    }

    @Test
    void test_constructor_with_missing_ok_http_client() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                null, httpBaseUrl + WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, CLIENT_SECRET));
    }

    @Test
    void test_constructor_with_missing_well_known_url() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), null, CLIENT_ID, CLIENT_SECRET));
    }

    @Test
    void test_constructor_with_missing_well_client_id() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + WELL_KNOWN_OPENID_CONFIGURATION, null, CLIENT_SECRET));
    }

    @Test
    void test_constructor_with_missing_well_client_secret() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, null));
    }

    @Test
    void test_constructor_nominal_case() {
        OAuth2ClientCredentialsFlowAuthenticator authenticator = new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, CLIENT_SECRET);
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(OAuth2ClientCredentialsFlowAuthenticator.class);
    }

    @Test
    void test_constructor_nominal_case_with_known_scopes() {
        OAuth2ClientCredentialsFlowAuthenticator authenticator = new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, CLIENT_SECRET, "openid", "email");
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(OAuth2ClientCredentialsFlowAuthenticator.class);
    }

    @Test
    void test_constructor_nominal_case_with_unknown_scopes() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, CLIENT_SECRET, "opensid", "emaissssl"));
    }
    @Test
    void test_authenticate_nominal_case() throws IOException {
        Authenticator authenticator = new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, CLIENT_SECRET);
        Route route = mock(Route.class);
        Request request = new Request.Builder().url(httpBaseUrl+SONG_PATH).get().build();
        Response.Builder builder = new Response.Builder();
        builder.setRequest$okhttp(request);
        Response response = builder.code(200).protocol(Protocol.HTTP_1_1).message("OK").build();
        Request authenticatedRequest = authenticator.authenticate(route, response);
        String authorizationHeader = authenticatedRequest.headers().get("Authorization");
        assertThat(authorizationHeader).isEqualTo("Bearer BQDzs98uhifaGayk8H9tCTRozufhFmgV_HKMCnnDdMTdz1FcOo3sdj8OZJ_azo96LRdLI9_1uJOCXxbGZme11KCb6ZxTuCt8B5FxEeECb1kO_-UDuf8");
    }

    @Test
    void test_authenticate_with_bad_well_known_content() {
        Assertions.assertThrows(RuntimeException.class,()->new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + BAD_WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, CLIENT_SECRET));
    }

    @Test
    void test_authenticate_with_bad_token_in_well_known_content() {
        Authenticator authenticator = new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl + BAD_TOKEN_WELL_KNOWN_OPENID_CONFIGURATION, CLIENT_ID, CLIENT_SECRET);

        Route route = mock(Route.class);
        Request request = new Request.Builder().url(httpBaseUrl+SONG_PATH).get().build();
        Response.Builder builder = new Response.Builder();
        builder.setRequest$okhttp(request);
        Response response = builder.code(200).protocol(Protocol.HTTP_1_1).message("OK").build();

        Assertions.assertThrows(RuntimeException.class,()-> authenticator.authenticate(route, response));
    }

}