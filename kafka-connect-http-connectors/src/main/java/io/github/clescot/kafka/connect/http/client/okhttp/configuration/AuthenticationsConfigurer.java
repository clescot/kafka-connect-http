package io.github.clescot.kafka.connect.http.client.okhttp.configuration;

import com.burgstaller.okhttp.*;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.PROXY_PREFIX;

/**
 * configure authentication settings for {@link OkHttpClient}.
 */
public class AuthenticationsConfigurer {
    private final List<AuthenticationConfigurer> authenticationConfigurers;

    public AuthenticationsConfigurer(List<AuthenticationConfigurer> authenticationConfigurers) {
        this.authenticationConfigurers = authenticationConfigurers;
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


        for (AuthenticationConfigurer configurer : authenticationConfigurers) {
            authenticatorBuilder = authenticatorBuilder.with(configurer.authenticationScheme(), configurer.configureAuthenticator(config));
        }

        //cache
        CachingAuthenticatorDecorator authenticator = null;
        Optional<Boolean> needCache = authenticationConfigurers.stream().map(configurer -> configurer.needCache()).findFirst();
        if (needCache.isPresent()&&needCache.get()) {
            authenticator = new CachingAuthenticatorDecorator(authenticatorBuilder.build(), authCache, proxy);
        }
        return authenticator;

    }

}
