package io.github.clescot.client.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import static io.github.clescot.client.Constants.*;

public class HttpClientConfigDefinition {
    private final Map<String, String> settings;



    public HttpClientConfigDefinition(Map<String, String> settings) {
        this.settings = settings;
    }

    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef()
                //custom configurations
                .define(CONFIGURATION_IDS, ConfigDef.Type.LIST, Lists.newArrayList(), ConfigDef.Importance.LOW, CONFIGURATION_IDS_DOC);
        //custom configurations
        String configurationIds = settings.get(CONFIGURATION_IDS);
        Set<String> configs = Sets.newHashSet();
        if (configurationIds != null) {
            configs.addAll(Arrays.asList(configurationIds.split(",")));
        }
        configs.add("default");
        for (String configurationName : configs) {
            configDef = appendConfigurationConfigDef(configDef, configurationName);
        }
        return configDef;
    }

    private ConfigDef appendConfigurationConfigDef(ConfigDef configDef, String configurationName) {
        String prefix = "config." + configurationName + ".";
        return configDef//http client implementation settings
                .define(prefix + HTTP_CLIENT_IMPLEMENTATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_IMPLEMENTATION_DOC)
                //random
                .define(prefix + HTTP_CLIENT_SECURE_RANDOM_ACTIVATE, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_ACTIVATE_DOC)
                .define(prefix + HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM, ConfigDef.Type.STRING, "SHA1PRNG", ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM_DOC)
                .define(prefix + HTTP_CLIENT_UNSECURE_RANDOM_SEED, ConfigDef.Type.LONG, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_UNSECURE_RANDOM_SEED_DOC)


                //SSL settings
                .define(prefix + HTTP_CLIENT_SSL_KEYSTORE_PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH_DOC)
                .define(prefix + HTTP_CLIENT_SSL_KEYSTORE_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD_DOC)
                .define(prefix + HTTP_CLIENT_SSL_KEYSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE_DOC)
                .define(prefix + HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM_DOC)
                .define(prefix + HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST_DOC)
                .define(prefix + HTTP_CLIENT_SSL_TRUSTSTORE_PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH_DOC)
                .define(prefix + HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD_DOC)
                .define(prefix + HTTP_CLIENT_SSL_TRUSTSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE_DOC)
                .define(prefix + HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM_DOC)
                //authentication
                //basic
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_BASIC_USER_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET, ConfigDef.Type.STRING, StandardCharsets.ISO_8859_1.name(), ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET_DOC)
                //digest
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_DIGEST_USER_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET, ConfigDef.Type.STRING, StandardCharsets.US_ASCII.name(), ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET_DOC)
                //OAuth2 Client Credentials Flow
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM_DOC)
                .define(prefix + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES_DOC)
                //proxy authentication
                //basic
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE_DOC)
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_BASIC_USER_DOC)
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD_DOC)
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET, ConfigDef.Type.STRING, StandardCharsets.ISO_8859_1.name(), ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET_DOC)
                //digest
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE_DOC)
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_DIGEST_USER_DOC)
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD_DOC)
                .define(prefix + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET, ConfigDef.Type.STRING, StandardCharsets.US_ASCII.name(), ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET_DOC)
                //proxy
                .define(prefix + PROXY_HTTP_CLIENT_HOSTNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_HTTP_CLIENT_HOSTNAME_DOC)
                .define(prefix + PROXY_HTTP_CLIENT_PORT, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_HTTP_CLIENT_PORT_DOC)
                .define(prefix + PROXY_HTTP_CLIENT_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_HTTP_CLIENT_TYPE_DOC)
                //proxy selector
                .define(prefix + PROXY_SELECTOR_ALGORITHM, ConfigDef.Type.STRING, "uriregex", ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_SELECTOR_ALGORITHM_DOC)
                .define(prefix + PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME_DOC)
                .define(prefix + PROXY_SELECTOR_HTTP_CLIENT_0_PORT, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_PORT_DOC)
                .define(prefix + PROXY_SELECTOR_HTTP_CLIENT_0_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_TYPE_DOC)
                .define(prefix + PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX_DOC)
                .define(prefix + PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX_DOC)

                //'okhttp' settings
                //cache
                .define(prefix + OKHTTP_CACHE_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE_DOC)
                .define(prefix + OKHTTP_CACHE_MAX_SIZE, ConfigDef.Type.LONG, 0, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE_DOC)
                .define(prefix + OKHTTP_CACHE_TYPE, ConfigDef.Type.STRING, "file", ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_CACHE_TYPE_DOC)
                .define(prefix + OKHTTP_CACHE_DIRECTORY_PATH, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH_DOC)

                //connection
                .define(prefix + OKHTTP_CALL_TIMEOUT, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT_DOC)
                .define(prefix + OKHTTP_READ_TIMEOUT, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT_DOC)
                .define(prefix + OKHTTP_CONNECT_TIMEOUT, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT_DOC)
                .define(prefix + OKHTTP_WRITE_TIMEOUT, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT_DOC)
                .define(prefix + OKHTTP_PROTOCOLS, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_PROTOCOLS_DOC)
                .define(prefix + OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION_DOC)
                .define(prefix + OKHTTP_RETRY_ON_CONNECTION_FAILURE, ConfigDef.Type.BOOLEAN, TRUE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_RETRY_ON_CONNECTION_FAILURE_DOC)
                //connection pool
                .define(prefix + OKHTTP_CONNECTION_POOL_SCOPE, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_SCOPE_DOC)
                .define(prefix + OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DOC)
                .define(prefix + OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION, ConfigDef.Type.LONG, 0, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION_DOC)

                //dispatcher
                .define(prefix + OKHTTP_DISPATCHER_MAX_REQUESTS, ConfigDef.Type.INT, 64, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKKHTTP_DISPATCHER_MAX_REQUESTS_DOC)
                .define(prefix + OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST, ConfigDef.Type.INT, 5, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST_DOC)

                //follow redirect
                .define(prefix + OKHTTP_FOLLOW_REDIRECT, ConfigDef.Type.STRING, TRUE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT_DOC)
                .define(prefix + OKHTTP_FOLLOW_SSL_REDIRECT, ConfigDef.Type.STRING, TRUE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT_DOC)

                //interceptors
                .define(prefix + OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE, ConfigDef.Type.STRING, TRUE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE_DOC)
                .define(prefix + OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE_DOC)
                .define(prefix + OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE_DOC);

    }

    public static Map<String, String> getMapWithPrefix(Map<String, String> map, String prefix) {
        return map.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(java.util.stream.Collectors.toMap(
                        entry -> entry.getKey().substring(prefix.length()),
                        Map.Entry::getValue
                ));
    }
}
