package io.github.clescot.kafka.connect.http.client.proxy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.net.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;

public class ProxySelectorFactory {

    public static final String URIREGEX_ALGORITHM = "uriregex";
    public static final String ROUNDROBIN_ALGORITHM = "roundrobin";
    public static final String RANDOM_ALGORITHM = "random";
    public static final String WEIGHTEDRANDOM_ALGORITHM = "weightedrandom";
    public static final String HOSTHASH_ALGORITHM = "hosthash";
    public static final String HOSTNAME = "hostname";

    public ProxySelector build(Map<String, String> config, Random random){
        Preconditions.checkNotNull(config,"config map is null");
        Preconditions.checkNotNull(random,"random is null");

        //handle NON_PROXY_HOSTS
        Optional<String> nonProxyHostNames = Optional.ofNullable(config.get(PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX));
        Pattern nonProxyHostsuriPattern = null;
        if (nonProxyHostNames.isPresent()) {
            String nonProxyHosts = nonProxyHostNames.get();
            nonProxyHostsuriPattern = Pattern.compile(nonProxyHosts);
        }

        ProxySelector proxySelector = getProxySelector(config,random);
        return nonProxyHostsuriPattern != null ? new ProxySelectorDecorator(proxySelector, nonProxyHostsuriPattern) : proxySelector;
    }

    private ProxySelector getProxySelector(Map<String, String> config,Random random) {
        String proxySelectorImpl = Optional.ofNullable(config.get(PROXY_SELECTOR_ALGORITHM)).orElse(URIREGEX_ALGORITHM);
        ProxySelector proxySelector;
        switch (proxySelectorImpl) {
            case HOSTHASH_ALGORITHM:
                proxySelector = buildHostHashProxySelector(config);
                break;
            case RANDOM_ALGORITHM:
                proxySelector = buildRandomProxySelector(config, random);
                break;
            case ROUNDROBIN_ALGORITHM:
                proxySelector = buildRoundRobinProxySelector(config);
                break;
            case URIREGEX_ALGORITHM:
                proxySelector = buildUriRegexProxySelector(config);
                break;
            case WEIGHTEDRANDOM_ALGORITHM:
                proxySelector = buildWeightedRandomProxySelector(config,random);
                break;
            default:
                proxySelector = buildUriRegexProxySelector(config);
                break;
        }
        return proxySelector;
    }

    private ProxySelector buildWeightedRandomProxySelector(Map<String, String> config,Random random) {
        return new WeightedRandomProxySelector(getWeightedProxies(config), random);
    }

    private NavigableMap<Integer, Proxy> getWeightedProxies(Map<String, String> config) {
        NavigableMap<Integer, Proxy> proxies = new TreeMap<>();
        int proxyIndex = 0;

        while (config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + HOSTNAME) != null) {
            //build proxy
            String proxyHostName = config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + HOSTNAME);
            int proxyPort = Integer.parseInt(config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "port"));
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = Optional.ofNullable(config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "type")).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            Proxy proxy = new Proxy(proxyType, socketAddress);
            //weight
            Integer proxyWeight = Integer.parseInt(config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "weight"));
            Preconditions.checkNotNull(proxyWeight, "'weight' for proxy host '" + proxyHostName + "' cannot be null in a WeightedRandomProxySelector");
            proxies.put(proxyWeight, proxy);
            proxyIndex++;
        }
        return proxies;
    }

    private ProxySelector buildHostHashProxySelector(Map<String, String> config) {
        return new HostHashProxySelector(getProxies(config));
    }

    private ProxySelector buildRandomProxySelector(Map<String, String> config, Random random) {
        //build each proxy
        //type,hostname,port
        List<Proxy> proxies = getProxies(config);
        return new RandomProxySelector(proxies, random);
    }
    private ProxySelector buildRoundRobinProxySelector(Map<String, String> config) {
        //build each proxy
        //type,hostname,port
        List<Proxy> proxies = getProxies(config);
        return new RoundRobinProxySelector(proxies);
    }

    private List<Proxy> getProxies(Map<String, String> config) {
        List<Proxy> proxies = Lists.newArrayList();
        int proxyIndex = 0;

        while (config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + HOSTNAME) != null) {
            //build proxy
            String proxyHostName = config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + HOSTNAME);
            int proxyPort = Integer.parseInt(config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "port"));
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = Optional.ofNullable(config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "type")).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            Proxy proxy = new Proxy(proxyType, socketAddress);
            proxies.add(proxy);
            proxyIndex++;
        }
        return proxies;
    }

    private URIRegexProxySelector buildUriRegexProxySelector(Map<String, String> config) {
        //build each proxy
        //type,hostname,port
        List<ImmutablePair<Predicate<URI>, Proxy>> proxies = Lists.newArrayList();
        int proxyIndex = 0;

        while (config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + HOSTNAME) != null) {
            //build URI predicate
            String uriPredicate = config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "uri.regex");
            Preconditions.checkNotNull(uriPredicate, "'" + PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "uri.regex" + "' must be set");
            Pattern uriPattern = Pattern.compile(uriPredicate);
            Predicate<URI> predicate = uri -> uriPattern.matcher(uri.toString()).matches();
            //build proxy
            String proxyHostName = config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + HOSTNAME);
            int proxyPort = Integer.parseInt(config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "port"));
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = Optional.ofNullable(config.get(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + proxyIndex + "." + "type")).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            Proxy proxy = new Proxy(proxyType, socketAddress);

            proxies.add(ImmutablePair.of(predicate, proxy));
            proxyIndex++;

        }
        return new URIRegexProxySelector(proxies);
    }

}
