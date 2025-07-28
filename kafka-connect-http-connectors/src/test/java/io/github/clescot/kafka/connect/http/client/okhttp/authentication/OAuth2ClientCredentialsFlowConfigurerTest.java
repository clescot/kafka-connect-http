package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Maps;
import okhttp3.Authenticator;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class OAuth2ClientCredentialsFlowConfigurerTest {
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
    void test_constructor_with_null_ok_http_client(){
        Assertions.assertThrows(NullPointerException.class,()->new OAuth2ClientCredentialsFlowConfigurer(null));
    }

    @Test
    void test_authentication_scheme(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        assertThat(authenticationConfigurer.authenticationScheme()).isEqualTo("Bearer");
    }

    @Test
    void test_need_cache(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        assertThat(authenticationConfigurer.needCache()).isTrue();
    }

    @Test
    void test_configure_nominal_case(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL,httpBaseUrl+"/.well-known/openid-configuration");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID,"1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET,"secret!1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD,"client_secret_basic");
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(OAuth2ClientCredentialsFlowAuthenticator.class);
    }

    @Test
    void test_configure_with_scopes(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL,httpBaseUrl+"/.well-known/openid-configuration");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID,"1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET,"secret!1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES,"openid,profile,email");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD,"client_secret_basic");
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(OAuth2ClientCredentialsFlowAuthenticator.class);
    }

    @Test
    void test_configure_with_some_scopes(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL,httpBaseUrl+"/.well-known/openid-configuration");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID,"1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET,"secret!1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES,"openid,email");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD,"client_secret_basic");
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(OAuth2ClientCredentialsFlowAuthenticator.class);
    }

    @Test
    void test_configure_with_one_unknown_scope(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL,httpBaseUrl+"/.well-known/openid-configuration");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID,"1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET,"secret!1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES,"openid,test");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD,"client_secret_basic");
        Assertions.assertThrows(IllegalArgumentException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_configure_with_missing_well_known_url(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID,"1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET,"secret!1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD,"client_secret_basic");
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_configure_with_missing_client_id(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL,httpBaseUrl+"/.well-known/openid-configuration");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET,"secret!1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD,"client_secret_basic");
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_configure_with_missing_client_secret(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL,httpBaseUrl+"/.well-known/openid-configuration");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID,"1234");
        config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD,"client_secret_basic");
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }



    @Test
    void test_configure_with_empty_map(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Map<String,Object> config = Maps.newHashMap();
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator)
                .isNull();
    }
    @Test
    void test_configure_with_null_map(){
        AuthenticationConfigurer authenticationConfigurer = new OAuth2ClientCredentialsFlowConfigurer(new OkHttpClient());
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(null));
    }
}