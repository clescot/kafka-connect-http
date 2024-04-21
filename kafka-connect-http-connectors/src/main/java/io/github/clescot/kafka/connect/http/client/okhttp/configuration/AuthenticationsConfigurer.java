package io.github.clescot.kafka.connect.http.client.okhttp.configuration;

import com.burgstaller.okhttp.*;
import com.burgstaller.okhttp.basic.BasicAuthenticator;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.burgstaller.okhttp.digest.DigestAuthenticator;
import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import okhttp3.Authenticator;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

/**
 * configure authentication settings for {@link OkHttpClient}.
 */
public class AuthenticationsConfigurer {
    public static final String US_ASCII = "US-ASCII";
    public static final String ISO_8859_1 = "ISO-8859-1";
    private final Random random;

    public AuthenticationsConfigurer(Random random) {
        this.random = random;
    }

    public void configure(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();

        //authentication
        CachingAuthenticatorDecorator authenticator = getCachingAuthenticatorDecorator(config, authCache, false);
        if (authenticator != null) {
            httpClientBuilder.authenticator(authenticator);
        }

        //proxy authentication
        Map<String, Object> proxyConfig = config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PROXY_PREFIX))
                .map(entry -> Map.entry(entry.getKey().substring(PROXY_PREFIX.length()), entry.getValue())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        CachingAuthenticatorDecorator proxyAuthenticator = getCachingAuthenticatorDecorator(proxyConfig, authCache, true);
        if (proxyAuthenticator != null) {
            httpClientBuilder.proxyAuthenticator(proxyAuthenticator);
        }

        //authentication cache
        if (proxyAuthenticator != null) {
            httpClientBuilder.addNetworkInterceptor(new AuthenticationCacheInterceptor(authCache, new DefaultProxyCacheKeyProvider()));
        }
        if (authenticator != null) {
            httpClientBuilder.addInterceptor(new AuthenticationCacheInterceptor(authCache, new DefaultRequestCacheKeyProvider()));
        }

    }

    @Nullable
    private CachingAuthenticatorDecorator getCachingAuthenticatorDecorator(Map<String, Object> config, Map<String, CachingAuthenticator> authCache, boolean proxy) {

        // note that all auth schemes should be registered as lowercase!
        DispatchingAuthenticator.Builder authenticatorBuilder = new DispatchingAuthenticator.Builder();

        //basic
        AuthenticationConfigurer basicAuthenticationConfigurer = new BasicAuthenticationConfigurer();
        Authenticator basicAuthenticator = basicAuthenticationConfigurer.configureAuthenticator(config);
        if (basicAuthenticator != null) {
            authenticatorBuilder = authenticatorBuilder.with(basicAuthenticationConfigurer.authenticationScheme(), basicAuthenticator);
        }

        //digest
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(this.random);
        Authenticator digestAuthenticator = authenticationConfigurer.configureAuthenticator(config);
        if (digestAuthenticator != null) {
            authenticatorBuilder = authenticatorBuilder.with(authenticationConfigurer.authenticationScheme(), digestAuthenticator);
        }

        //cache
        CachingAuthenticatorDecorator authenticator = null;
        if (basicAuthenticator != null || digestAuthenticator != null) {
            authenticator = new CachingAuthenticatorDecorator(authenticatorBuilder.build(), authCache, proxy);
        }
        return authenticator;

    }

    @Nullable
    private DigestAuthenticator configureDigestAuthenticator(Map<String, Object> config) {
        //Digest Authentication
        DigestAuthenticator digestAuthenticator = null;
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE) && Boolean.TRUE.equals(config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE))) {
            String username = (String) config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME);
            Preconditions.checkNotNull(username,"'"+HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME+"' is null");
            String password = (String) config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD);
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

    @Nullable
    private BasicAuthenticator configureBasicAuthentication(Map<String, Object> config) {
            //Basic authentication
            BasicAuthenticator basicAuthenticator = null;
            if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE) && Boolean.TRUE.equals(config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE))) {
                String username = (String) config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME);
                String password = (String) config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD);
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
