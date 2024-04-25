package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.google.common.base.Preconditions;
import okhttp3.Authenticator;
import okhttp3.OkHttpClient;

import java.util.Map;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class OAuth2ClientCredentialsFlowConfigurer implements AuthenticationConfigurer{


    private final OkHttpClient okHttpClient;

    public OAuth2ClientCredentialsFlowConfigurer(OkHttpClient okHttpClient) {
        this.okHttpClient = okHttpClient;
    }

    @Override
    public String authenticationScheme() {
        return "Bearer";
    }

    @Override
    public boolean needCache() {
        return true;
    }

    @Override
    public Authenticator configureAuthenticator(Map<String, Object> config) {
        Authenticator authenticator = null;
        Preconditions.checkNotNull(config,"config map is null");
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE) && Boolean.TRUE.equals(config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE))){
            String wellKnownUrl = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL).toString();
            String clientId =config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID).toString();
            String clientSecret = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET).toString();
            Object configuredScopes = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES);
            String[] scopes = null;
            if(configuredScopes!=null) {
                scopes = configuredScopes.toString().split(",");
            }
            authenticator = new OAuth2ClientCredentialsFlowAuthenticator(okHttpClient,wellKnownUrl,clientId,clientSecret,scopes);
        }
        return authenticator;
    }
}
