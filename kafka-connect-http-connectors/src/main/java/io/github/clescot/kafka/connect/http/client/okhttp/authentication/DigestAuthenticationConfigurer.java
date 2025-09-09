package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.burgstaller.okhttp.digest.DigestAuthenticator;
import com.google.common.base.Preconditions;
import okhttp3.Authenticator;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;

public class DigestAuthenticationConfigurer implements AuthenticationConfigurer{

    public static final String US_ASCII = "US-ASCII";
    private final Random random;


    public DigestAuthenticationConfigurer(Random random) {
        this.random = random;
    }

    @Override
    public String authenticationScheme() {
        return "digest";
    }

    @Override
    public boolean needCache() {
        return true;
    }

    @Override
    public Authenticator configureAuthenticator(Map<String, String> config) {
        //Digest Authentication
        DigestAuthenticator digestAuthenticator = null;
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE) && Boolean.TRUE.equals(Boolean.parseBoolean(config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE)))) {
            String username = config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME);
            Preconditions.checkNotNull(username,"'"+HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME+"' is null");
            String password = config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD);
            Preconditions.checkNotNull(password,"'"+HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD+"' is null");
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(username, password);
            //digest charset
            String digestCredentialCharset = US_ASCII;
            if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET)) {
                digestCredentialCharset = String.valueOf(config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET));
            }
            Charset digestCharset = Charset.forName(digestCredentialCharset);


            digestAuthenticator = new DigestAuthenticator(credentials, digestCharset, random);

        }
        return digestAuthenticator;
    }
}
