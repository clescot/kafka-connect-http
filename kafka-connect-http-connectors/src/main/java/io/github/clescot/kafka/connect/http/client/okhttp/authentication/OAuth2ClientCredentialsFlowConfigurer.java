package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import okhttp3.Authenticator;

import java.util.Map;

public class OAuth2ClientCredentialsFlowConfigurer implements AuthenticationConfigurer{
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

        return null;
    }
}
