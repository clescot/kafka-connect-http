package io.github.clescot.kafka.connect.http.client.okhttp.configuration;

import com.burgstaller.okhttp.*;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import okhttp3.Authenticator;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.PROXY_PREFIX;

/**
 * configure authentication settings for {@link OkHttpClient}.
 */
public class AuthenticationsConfigurer {
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

}
