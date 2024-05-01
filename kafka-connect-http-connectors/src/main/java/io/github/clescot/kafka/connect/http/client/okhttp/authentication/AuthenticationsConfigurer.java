package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.burgstaller.okhttp.*;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import okhttp3.Authenticator;
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
        Preconditions.checkNotNull(authenticationConfigurers, "authenticationConfigurers is null");
        Preconditions.checkArgument(!authenticationConfigurers.isEmpty(), "authenticationConfigurers is empty");
        this.authenticationConfigurers = authenticationConfigurers;
    }

    public void configure(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();

        //authentication
        Authenticator authenticator = getAuthenticatorDecorator(config, authCache, false);
        if (authenticator != null) {
            httpClientBuilder.authenticator(authenticator);
            httpClientBuilder.addInterceptor(new AuthenticationCacheInterceptor(authCache, new DefaultRequestCacheKeyProvider()));
        }

        //proxy authentication
        Map<String, Object> proxyConfig = config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PROXY_PREFIX))
                .map(entry -> Map.entry(entry.getKey().substring(PROXY_PREFIX.length()), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!proxyConfig.isEmpty()) {
            Authenticator proxyAuthenticator = getAuthenticatorDecorator(proxyConfig, authCache, true);
            if (proxyAuthenticator != null) {
                httpClientBuilder.proxyAuthenticator(proxyAuthenticator);
                //authentication cache
                httpClientBuilder.addNetworkInterceptor(new AuthenticationCacheInterceptor(authCache, new DefaultProxyCacheKeyProvider()));
            }
        }


    }

    @Nullable
    protected Authenticator getAuthenticatorDecorator(Map<String, Object> config, Map<String, CachingAuthenticator> authCache, boolean proxy) {

        // note that all auth schemes should be registered as lowercase!
        DispatchingAuthenticator.Builder authenticatorBuilder = new DispatchingAuthenticator.Builder();

        for (AuthenticationConfigurer configurer : authenticationConfigurers) {
            authenticatorBuilder = authenticatorBuilder.with(configurer.authenticationScheme(), configurer.configureAuthenticator(config));
        }

        //cache
        Authenticator authenticator;
        Optional<Boolean> needCache = authenticationConfigurers.stream().map(AuthenticationConfigurer::needCache).findFirst();
        DispatchingAuthenticator dispatchingAuthenticator = authenticatorBuilder.build();
        if (needCache.isPresent() && Boolean.TRUE.equals(needCache.get())) {
            authenticator = new CachingAuthenticatorDecorator(dispatchingAuthenticator, authCache, proxy);
        } else {
            authenticator = dispatchingAuthenticator;
        }
        return authenticator;

    }

}
