package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthenticator implements Authenticator {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthenticator.class);
    public static final String AUTHORIZATION = "Authorization";
    private final String username;
    private final String password;

    public BasicAuthenticator(String username,
                              String password) {
        this.username = username;
        this.password = password;
    }

    @Nullable
    @Override
    public Request authenticate(@Nullable Route route, @NotNull Response response) {
        if (response.request().header(AUTHORIZATION) != null) {
            return null; // Give up, we've already attempted to authenticate.
        }

        LOGGER.debug("Authenticating for response: '{}'",response);
        LOGGER.debug("Challenges: '{}'",response.challenges());
        String credential = Credentials.basic(username, password);
        return response.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build();
    }


    public String getAuthenticationScheme(){
        //according to https://www.iana.org/assignments/http-authschemes/http-authschemes.xhtml
        return "Basic";
    }
}
