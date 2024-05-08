package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.burgstaller.okhttp.basic.BasicAuthenticator;
import com.google.common.base.Preconditions;
import okhttp3.Authenticator;

import java.nio.charset.Charset;
import java.util.Map;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET;

public class BasicAuthenticationConfigurer implements AuthenticationConfigurer{
    public static final String ISO_8859_1 = "ISO-8859-1";

    @Override
    public String authenticationScheme() {
        return "basic";
    }

    @Override
    public boolean needCache() {
        return true;
    }

    @Override
    public Authenticator configureAuthenticator(Map<String, Object> config) {
        Preconditions.checkNotNull(config,"config map is null");
        //Basic authentication
        BasicAuthenticator basicAuthenticator = null;
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE) && Boolean.TRUE.equals(config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE))) {
            String username = (String) config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME);
            Preconditions.checkNotNull(username,HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME+" must be set");
            String password = (String) config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD);
            Preconditions.checkNotNull(password,HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD+" must be set");
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(username, password);


            //basic charset
            String basicCredentialCharset = ISO_8859_1;
            if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET)) {
                basicCredentialCharset = String.valueOf(config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET));
            }
            Charset basicCharset = Charset.forName(basicCredentialCharset);
            basicAuthenticator = new BasicAuthenticator(credentials, basicCharset);
        }
        return basicAuthenticator;
    }
}
