package io.github.clescot.kafka.connect.http.client.okhttp.authentication;


import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;

class OAuth2ClientCredentialsFlowAuthenticatorTest {
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
    private String httpBaseUrl;
    @BeforeEach
    void setup() throws IOException {
        String scenario = "test_successful_request_at_first_time";
        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        Path path = Paths.get("src/test/resources/oauth2/wellknownUrlContent.json");
        httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
        String content = Files.readString(path);
        String wellKnownUrlContent = content.replaceAll("baseUrl",httpBaseUrl);

        wireMock
                .register(WireMock.get("/.well-known/openid-configuration").inScenario(scenario)
                        .whenScenarioStateIs(STARTED)
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)
                                .withStatusMessage("OK")
                                .withBody(wellKnownUrlContent)
                        ).willSetStateTo(WELL_KNOWN_OK)
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
                null, httpBaseUrl+"/.well-known/openid-configuration", "1234", "1234?/"));
    }

    @Test
    void test_constructor_with_missing_well_known_url() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), null, "1234", "1234?/"));
    }

    @Test
    void test_constructor_with_missing_well_client_id() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl+"/.well-known/openid-configuration", null, "1234?/"));
    }

    @Test
    void test_constructor_with_missing_well_client_secret() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl+"/.well-known/openid-configuration", "1234", null));
    }

    @Test
    void test_constructor_nominal_case() {
        OAuth2ClientCredentialsFlowAuthenticator authenticator = new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl+"/.well-known/openid-configuration", "1234", "1234?/");
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(OAuth2ClientCredentialsFlowAuthenticator.class);
    }

    @Test
    void test_constructor_nominal_case_with_known_scopes() {
        OAuth2ClientCredentialsFlowAuthenticator authenticator = new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl+"/.well-known/openid-configuration", "1234", "1234?/","openid","email");
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(OAuth2ClientCredentialsFlowAuthenticator.class);
    }
  @Test
    void test_constructor_nominal_case_with_unknown_scopes() {
      Assertions.assertThrows(IllegalArgumentException.class,()->new OAuth2ClientCredentialsFlowAuthenticator(
                new OkHttpClient(), httpBaseUrl+"/.well-known/openid-configuration", "1234", "1234?/","opensid","emaissssl"));
    }


}