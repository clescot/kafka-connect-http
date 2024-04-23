package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import okhttp3.Authenticator;

import java.util.Map;

public interface AuthenticationConfigurer {


    String authenticationScheme();

    boolean needCache();

    Authenticator configureAuthenticator(Map<String, Object> config);
}
