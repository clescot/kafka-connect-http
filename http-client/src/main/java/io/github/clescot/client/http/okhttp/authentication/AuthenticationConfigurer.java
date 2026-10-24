package io.github.clescot.client.http.okhttp.authentication;

import okhttp3.Authenticator;

import java.util.Map;

public interface AuthenticationConfigurer {


    String authenticationScheme();

    boolean needCache();

    Authenticator configureAuthenticator(Map<String, String> config);
}
