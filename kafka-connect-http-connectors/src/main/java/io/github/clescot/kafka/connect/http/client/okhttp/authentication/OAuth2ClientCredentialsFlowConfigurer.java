package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.google.common.base.Preconditions;
import okhttp3.Authenticator;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class OAuth2ClientCredentialsFlowConfigurer implements AuthenticationConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2ClientCredentialsFlowConfigurer.class);
    private final OkHttpClient okHttpClient;

    public OAuth2ClientCredentialsFlowConfigurer(OkHttpClient okHttpClient) {
        Preconditions.checkNotNull(okHttpClient, "okHttp is null");
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
        Preconditions.checkNotNull(config, "config map is null");
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE)
                && config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE) != null
                && Boolean.TRUE.equals(Boolean.parseBoolean(config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE).toString()))) {

            Object wellKnownObject = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL);
            Preconditions.checkNotNull(wellKnownObject, HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL + " is null");
            String wellKnownUrl = wellKnownObject.toString();


            Object configuredScopes = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES);
            String[] scopes = null;
            if (configuredScopes != null) {
                scopes = configuredScopes.toString().split(",");
            }

            authenticator = new OAuth2ClientCredentialsFlowAuthenticator(okHttpClient, wellKnownUrl, config, scopes);
        }
        return authenticator;
    }




}
