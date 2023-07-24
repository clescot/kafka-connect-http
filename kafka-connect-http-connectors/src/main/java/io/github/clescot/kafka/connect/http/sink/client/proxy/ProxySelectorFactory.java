package io.github.clescot.kafka.connect.http.sink.client.proxy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.jetbrains.annotations.NotNull;

import java.net.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.HTTP_CLIENT_PREFIX;

public class ProxySelectorFactory {
    public ProxySelector build(Map<String, Object> config,Random random){
        //handle NON_PROXY_HOSTS
        Optional<String> nonProxyHostNames = Optional.ofNullable((String) config.get(PROXY_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX));
        Pattern nonProxyHostsuriPattern = null;
        if (nonProxyHostNames.isPresent()) {
            String nonProxyHosts = nonProxyHostNames.get();
            nonProxyHostsuriPattern = Pattern.compile(nonProxyHosts);
        }

        ProxySelector proxySelector = getProxySelector(config,random);
        return nonProxyHostsuriPattern != null ? new ProxySelectorDecorator(proxySelector, nonProxyHostsuriPattern) : proxySelector;
    }

    private ProxySelector getProxySelector(Map<String, Object> config,Random random) {
        String proxySelectorImpl = Optional.ofNullable((String) config.get(PROXY_SELECTOR_IMPLEMENTATION)).orElse("uriregex");
        ProxySelector proxySelector;
        switch (proxySelectorImpl) {
            case "weightedrandom":
                proxySelector = buildWeightedRandomProxySelector(config,random);
                break;
            case "hosthash":
                proxySelector = buildHostHashProxySelector(config);
                break;
            case "random":
                proxySelector = buildRandomProxySelector(config, random);
                break;
            case "uriregex":
                proxySelector = buildUriRegexProxySelector(config);
                break;
            default:
                proxySelector = buildUriRegexProxySelector(config);
                break;
        }
        return proxySelector;
    }

    private ProxySelector buildWeightedRandomProxySelector(Map<String, Object> config,Random random) {
        return new WeightedRandomProxySelector(getWeightedProxies(config), random);
    }

    private NavigableMap<Integer, Proxy> getWeightedProxies(Map<String, Object> config) {
        NavigableMap<Integer, Proxy> proxies = new TreeMap<>();
        int proxyIndex = 0;

        while (config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "hostname") != null) {
            //build proxy
            String proxyHostName = (String) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "hostname");
            int proxyPort = (Integer) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "port");
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = (String) Optional.ofNullable(config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "type")).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            Proxy proxy = new Proxy(proxyType, socketAddress);
            //weight
            Integer proxyWeight = (Integer) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "weight");
            Preconditions.checkNotNull(proxyWeight, "'weight' for proxy host '" + proxyHostName + "' cannot be null in a WeightedRandomProxySelector");
            proxies.put(proxyWeight, proxy);
            proxyIndex++;
        }
        return proxies;
    }

    private ProxySelector buildHostHashProxySelector(Map<String, Object> config) {
        return new HostHashProxySelector(getProxies(config));
    }

    private ProxySelector buildRandomProxySelector(Map<String, Object> config, Random random) {
        //build each proxy
        //type,hostname,port
        List<Proxy> proxies = getProxies(config);
        return new RandomProxySelector(proxies, random);
    }

    private List<Proxy> getProxies(Map<String, Object> config) {
        List<Proxy> proxies = Lists.newArrayList();
        int proxyIndex = 0;

        while (config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "hostname") != null) {
            //build proxy
            String proxyHostName = (String) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "hostname");
            int proxyPort = (Integer) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "port");
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = (String) Optional.ofNullable(config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "type")).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            Proxy proxy = new Proxy(proxyType, socketAddress);
            proxies.add(proxy);
            proxyIndex++;
        }
        return proxies;
    }

    @NotNull
    private URIRegexProxySelector buildUriRegexProxySelector(Map<String, Object> config) {
        //build each proxy
        //type,hostname,port
        List<ImmutablePair<Predicate<URI>, Proxy>> proxies = Lists.newArrayList();
        int proxyIndex = 0;

        while (config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "hostname") != null) {
            //build URI predicate
            String uriPredicate = (String) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "uri.regex");
            Preconditions.checkNotNull(uriPredicate, "'" + PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "uri.regex" + "' must be set");
            Pattern uriPattern = Pattern.compile(uriPredicate);
            Predicate<URI> predicate = uri -> uriPattern.matcher(uri.toString()).matches();
            //build proxy
            String proxyHostName = (String) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "hostname");
            int proxyPort = (Integer) config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "port");
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = (String) Optional.ofNullable(config.get(PROXY_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "type")).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            Proxy proxy = new Proxy(proxyType, socketAddress);

            proxies.add(ImmutablePair.of(predicate, proxy));
            proxyIndex++;

        }
        return new URIRegexProxySelector(proxies);
    }

}
