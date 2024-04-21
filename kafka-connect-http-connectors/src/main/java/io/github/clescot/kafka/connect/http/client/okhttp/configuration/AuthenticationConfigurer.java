package io.github.clescot.kafka.connect.http.client.okhttp.configuration;

import okhttp3.Authenticator;

import java.util.Map;

public interface AuthenticationConfigurer {


    String authenticationScheme();

    Authenticator configureAuthenticator(Map<String, Object> config);
}
