package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class HttpClientConfigDefinition {
    private final Map<String, String> settings;



    public static final String FALSE = "false";
    public static final String TRUE = "true";




    //http client prefix
    public static final String HTTP_CLIENT_PREFIX = "httpclient.";
    public static final String PROXY_PREFIX = "proxy.";
    public static final String PROXYSELECTOR_PREFIX = "proxyselector.";
    public static final String OKHTTP_PREFIX = "okhttp.";
    public static final String AHC_PREFIX = "ahc.";

    public static final String DEFAULT_CONFIGURATION_PREFIX = "config.default.";
    //configuration
    public static final String CONFIGURATION_IDS = "config.ids";
    public static final String CONFIGURATION_IDS_DOC = "custom configurations id list. 'default' configuration is already registered.";

    public static final String HTTP_CLIENT_IMPLEMENTATION = HTTP_CLIENT_PREFIX + "implementation";
    public static final String CONFIG_HTTP_CLIENT_IMPLEMENTATION = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_IMPLEMENTATION;
    public static final String CONFIG_HTTP_CLIENT_IMPLEMENTATION_DOC = "define which intalled library to use : either 'ahc', a.k.a async http client, or 'okhttp'. default is 'okhttp'.";

    public static final String OKHTTP_IMPLEMENTATION = "okhttp";
    public static final String AHC_IMPLEMENTATION = "ahc";

    //proxy
    public static final String PROXY_HTTP_CLIENT_HOSTNAME = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "hostname";
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_HOSTNAME = DEFAULT_CONFIGURATION_PREFIX + PROXY_HTTP_CLIENT_HOSTNAME;
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_HOSTNAME_DOC = "hostname of the proxy host.";

    public static final String PROXY_HTTP_CLIENT_PORT = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "port";
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_PORT = DEFAULT_CONFIGURATION_PREFIX + PROXY_HTTP_CLIENT_PORT;
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_PORT_DOC = "hostname of the proxy host.";

    public static final String PROXY_HTTP_CLIENT_TYPE = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "type";
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_TYPE = DEFAULT_CONFIGURATION_PREFIX + PROXY_HTTP_CLIENT_TYPE;
    public static final String CONFIG_DEFAULT_PROXY_HTTP_CLIENT_TYPE_DOC = "type of proxy. can be either 'HTTP' (default), 'DIRECT' (i.e no proxy), or 'SOCKS'";

    //proxy selector
    public static final String PROXY_SELECTOR_ALGORITHM = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "algorithm";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_ALGORITHM;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_ALGORITHM_DOC = "algorithm of the proxy selector.can be 'uriregex', 'random', 'weightedrandom', or 'hosthash'. Default is 'uriregex'.";


    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "hostname";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME_DOC = "hostname of the proxy host.";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_PORT = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "port";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_PORT = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_PORT;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_PORT_DOC = "hostname of the proxy host.";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_TYPE = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "type";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_TYPE = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_TYPE;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_TYPE_DOC = "type of proxy. can be either 'HTTP' (default), 'DIRECT' (i.e no proxy), or 'SOCKS'";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "0." + "uri.regex";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_0_URI_REGEX_DOC = "uri regex matching this proxy";

    public static final String PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX = PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX + "non.proxy.hosts.uri.regex";
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX = DEFAULT_CONFIGURATION_PREFIX + PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX;
    public static final String CONFIG_DEFAULT_PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX_DOC = "hosts which don't need to be proxied to be reached.";


    //okhttp settings
    //cache
    public static final String OKHTTP_CACHE_ACTIVATE = OKHTTP_PREFIX + "cache.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_ACTIVATE_DOC = "set to true to activate page cache (if cache hit, the server will not receive the request, and the response will comes from the cache). default is false.";

    public static final String OKHTTP_CACHE_MAX_SIZE = OKHTTP_PREFIX + "cache.max.size";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_MAX_SIZE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_MAX_SIZE_DOC = "max size of the page cache.";

    public static final String OKHTTP_CACHE_TYPE = OKHTTP_PREFIX + "cache.type";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_TYPE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_TYPE;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_TYPE_DOC = "persistance of the cache : either 'file'(default), or 'inmemory'.";

    public static final String OKHTTP_CACHE_DIRECTORY_PATH = OKHTTP_PREFIX + "cache.directory.path";
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CACHE_DIRECTORY_PATH;
    public static final String CONFIG_DEFAULT_OKHTTP_CACHE_DIRECTORY_PATH_DOC = "file system path of the cache directory.";

    //DNS over HTTPS
    public static final String OKHTTP_DOH_PREFIX=OKHTTP_PREFIX +"doh.";

    public static final String OKHTTP_DOH_ACTIVATE=OKHTTP_DOH_PREFIX +".activate";
    public static final String OKHTTP_DOH_ACTIVATE_DOC="resolve DNS domain with HTTPS if set to 'true'";

    public static final String OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS=OKHTTP_DOH_PREFIX +".bootstrap.dns.hosts";
    public static final String OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS_DOC="list of bootstrap dns";

    public static final String OKHTTP_DOH_INCLUDE_IPV6=OKHTTP_DOH_PREFIX +".include.ipv6";
    public static final String OKHTTP_DOH_INCLUDE_IPV6_DOC="include ipv6. default is 'true'";

    public static final String OKHTTP_DOH_USE_POST_METHOD=OKHTTP_DOH_PREFIX +".use.post.method";
    public static final String OKHTTP_DOH_USE_POST_METHOD_DOC="use HTTP 'POST' method instead of get. default is 'false'.";

    public static final String OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES=OKHTTP_DOH_PREFIX +".resolve.private.addresses";
    public static final String OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES_DOC="resolve private addresses. default is 'false'";

    public static final String OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES=OKHTTP_DOH_PREFIX +".resolve.public.addresses";
    public static final String OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES_DOC="resolve public addresses. default is 'true'";

    public static final String OKHTTP_DOH_URL=OKHTTP_DOH_PREFIX +".url";
    public static final String OKHTTP_DOH_URL_DOC="DNS Over HTTP url";

    //connection
    public static final String OKHTTP_CALL_TIMEOUT = OKHTTP_PREFIX + "call.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CALL_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_CALL_TIMEOUT_DOC = "default timeout in milliseconds for complete call . A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_READ_TIMEOUT = OKHTTP_PREFIX + "read.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_READ_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_READ_TIMEOUT_DOC = "Sets the default read timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_CONNECT_TIMEOUT = OKHTTP_PREFIX + "connect.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECT_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECT_TIMEOUT_DOC = "Sets the default connect timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_WRITE_TIMEOUT = OKHTTP_PREFIX + "write.timeout";
    public static final String CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_WRITE_TIMEOUT;
    public static final String CONFIG_DEFAULT_OKHTTP_WRITE_TIMEOUT_DOC = "Sets the default write timeout in milliseconds for new connections. A value of 0 means no timeout, otherwise values must be between 1 and Integer.MAX_VALUE.";

    public static final String OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION = OKHTTP_PREFIX + "ssl.skip.hostname.verification";
    public static final String CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION;
    public static final String CONFIG_DEFAULT_OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION_DOC = "if set to 'true', skip hostname verification. Not set by default.";

    public static final String OKHTTP_RETRY_ON_CONNECTION_FAILURE = OKHTTP_PREFIX + "retry.on.connection.failure";
    public static final String CONFIG_DEFAULT_OKHTTP_RETRY_ON_CONNECTION_FAILURE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_RETRY_ON_CONNECTION_FAILURE;
    public static final String CONFIG_DEFAULT_OKHTTP_RETRY_ON_CONNECTION_FAILURE_DOC = "if set to 'false', will not retry connection on connection failure. default is true";


    //protocols to use, in order of preference,divided by a comma.supported protocols in okhttp: HTTP_1_1,HTTP_2,H2_PRIOR_KNOWLEDGE,QUIC
    public static final String OKHTTP_PROTOCOLS = OKHTTP_PREFIX + "protocols";
    public static final String CONFIG_DEFAULT_OKHTTP_PROTOCOLS = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_PROTOCOLS;
    public static final String CONFIG_DEFAULT_OKHTTP_PROTOCOLS_DOC = "the protocols to use, in order of preference. If the list contains 'H2_PRIOR_KNOWLEDGE' then that must be the only protocol and HTTPS URLs will not be supported. Otherwise the list must contain 'HTTP_1_1'. The list must not contain null or 'HTTP_1_0'.";

    public static final String OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION = OKHTTP_PREFIX + "connection.pool.keep.alive.duration";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION_DOC = "Time in milliseconds to keep the connection alive in the pool before closing it. Default is 0 (no connection pool).";

    public static final String OKHTTP_CONNECTION_POOL_SCOPE = OKHTTP_PREFIX + "connection.pool.scope";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_SCOPE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECTION_POOL_SCOPE;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_SCOPE_DOC = "scope of the '" + CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_SCOPE + "' parameter. can be either 'instance' (i.e a connection pool per configuration in the connector instance),  or 'static' (a connection pool shared with all connectors instances in the same Java Virtual Machine).";

    public static final String OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS = OKHTTP_PREFIX + "connection.pool.max.idle.connections";
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS;
    public static final String CONFIG_DEFAULT_OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DOC = "amount of connections to keep idle, to avoid the connection creation time when needed. Default is 0 (no connection pool)";

    public static final String OKHTTP_FOLLOW_REDIRECT = OKHTTP_PREFIX + "follow.redirect";
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_FOLLOW_REDIRECT;
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT_DOC = "does the http client need to follow a redirect response from the server. default to true.";

    public static final String OKHTTP_FOLLOW_SSL_REDIRECT = OKHTTP_PREFIX + "follow.ssl.redirect";
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_FOLLOW_SSL_REDIRECT;
    public static final String CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT_DOC = "does the http client need to follow an SSL redirect response from the server. default to true.";

    public static final String OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE = OKHTTP_PREFIX + "interceptor.logging.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE_DOC = "activate tracing of request and responses via an okhttp network interceptor. 'true' and 'false' are accepted values. default is true";

    public static final String OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE = OKHTTP_PREFIX + "interceptor.inet.address.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE_DOC = "activate tracing of request and responses via an okhttp network interceptor. 'true' and 'false' are accepted values. default is true";

    public static final String OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE = OKHTTP_PREFIX + "interceptor.ssl.handshake.activate";
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE;
    public static final String CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE_DOC = "activate tracing of request and responses via an okhttp network interceptor. 'true' and 'false' are accepted values. default is true";

    //random
    public static final String HTTP_CLIENT_SECURE_RANDOM_ACTIVATE = HTTP_CLIENT_PREFIX + "secure.random.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SECURE_RANDOM_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_ACTIVATE_DOC = "if 'true', use a secure random instead of a pseudo random number generator.";


    public static final String HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM = HTTP_CLIENT_PREFIX + "secure.random.prng.algorithm";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM_DOC = "name of the Random Number Generator (RNG) algorithm used to get a Secure Random instance. if not set, 'SHA1PRNG' algorithm is used when the secure random generator is activated. cf https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#securerandom-number-generation-algorithms";

    public static final String HTTP_CLIENT_UNSECURE_RANDOM_SEED = HTTP_CLIENT_PREFIX + "unsecure.random.seed";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_UNSECURE_RANDOM_SEED = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_UNSECURE_RANDOM_SEED;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_UNSECURE_RANDOM_SEED_DOC = "seed used to build the unsecure random generator.";


    //SSL
    public static final String HTTP_CLIENT_SSL_KEYSTORE_PATH = HTTP_CLIENT_PREFIX + "ssl.keystore.path";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_PATH;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH_DOC = "file path of the custom key store.";

    public static final String HTTP_CLIENT_SSL_KEYSTORE_PASSWORD = HTTP_CLIENT_PREFIX + "ssl.keystore.password";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_PASSWORD;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD_DOC = "password of the custom key store.";

    public static final String HTTP_CLIENT_SSL_KEYSTORE_TYPE = HTTP_CLIENT_PREFIX + "ssl.keystore.type";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_TYPE;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE_DOC = "keystore type. can be 'jks' or 'pkcs12'.";

    public static final String HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM = HTTP_CLIENT_PREFIX + "ssl.keystore.algorithm";
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM;
    public static final String CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM_DOC = "the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST = HTTP_CLIENT_PREFIX + "ssl.truststore.always.trust";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST_DOC = "trust store that always trust any certificate. this option remove any security on the transport layer. be careful when you activate this option ! you will have no guarantee that you don't contact any hacked server ! ";


    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_PATH = HTTP_CLIENT_PREFIX + "ssl.truststore.path";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_PATH;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH_DOC = "file path of the custom trust store.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD = HTTP_CLIENT_PREFIX + "ssl.truststore.password";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD_DOC = "password of the custom trusted store.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_TYPE = HTTP_CLIENT_PREFIX + "ssl.truststore.type";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_TYPE;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE_DOC = "truststore type. can be 'jks' or 'pkcs12'.";

    public static final String HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM = HTTP_CLIENT_PREFIX + "ssl.truststore.algorithm";
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM;
    public static final String CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM_DOC = "the standard name of the requested algorithm. See the KeyManagerFactory section in the Java Security Standard Algorithm Names Specification for information about standard algorithm names.";

    //authentication
    //Basic
    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE = HTTP_CLIENT_PREFIX + "authentication.basic.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE_DOC = "activate the BASIC authentication";


    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME = HTTP_CLIENT_PREFIX + "authentication.basic.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_BASIC_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_BASIC_USER_DOC = "username for basic authentication";

    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD = HTTP_CLIENT_PREFIX + "authentication.basic.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD_DOC = "password for basic authentication";

    public static final String HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET = HTTP_CLIENT_PREFIX + "authentication.basic.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET_DOC = "charset used to encode basic credentials. default is 'ISO-8859-1'";

    //Digest
    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE = HTTP_CLIENT_PREFIX + "authentication.digest.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE_DOC = "activate the DIGEST authentication";


    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME = HTTP_CLIENT_PREFIX + "authentication.digest.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_DIGEST_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_AUTHENTICATION_DIGEST_USER_DOC = "username for digest authentication";

    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD = HTTP_CLIENT_PREFIX + "authentication.digest.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD_DOC = "password for digest authentication";


    public static final String HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET = HTTP_CLIENT_PREFIX + "authentication.digest.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET_DOC = "charset used to encode 'digest' credentials. default is 'US-ASCII'";

    //OAuth2
    //Client Credentials Flow
    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE_DOC = "activate the OAuth2 Client Credentials flow authentication, suited for Machine-To-Machine applications (M2M).";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.well.known.url";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL_DOC = "OAuth2 URL of the provider's Well-Known Configuration Endpoint.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.authentication.method";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD_DOC = "OAuth2 Client authentication method. either 'client_secret_basic', 'client_secret_post', or 'client_secret_jwt' are supported. default value is 'client_secret_basic'.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.id";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID_DOC = "Client id.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.secret";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET_DOC = "Client secret.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.issuer";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER_DOC = "Client issuer for JWT token.";

    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.client.jws.algorithm";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM_DOC = "JWS Algorithm for JWT token. default is 'HS256'.";


    public static final String HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES = HTTP_CLIENT_PREFIX + "authentication.oauth2.client.credentials.flow.scopes";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES_DOC = "optional scopes, splitted with a comma separator.";


    //proxy authentication
    //Basic on proxy
    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE_DOC = "activate the BASIC authentication for proxy.";


    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_BASIC_USER_DOC = "username for proxy basic authentication";

    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD_DOC = "password for proxy basic authentication";

    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.basic.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_CHARSET_DOC = "charset used to encode basic credentialsfor proxy. default is 'ISO-8859-1'";

    //Digest on proxy
    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.activate";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE_DOC = "activate the DIGEST authentication for proxy.";


    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.username";
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME;
    public static final String CONFIG_DEFAULT_HTTPCLIENT_PROXY_AUTHENTICATION_DIGEST_USER_DOC = "username for proxy digest authentication";

    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.password";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD_DOC = "password for proxy digest authentication";


    public static final String HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET = PROXY_PREFIX + HTTP_CLIENT_PREFIX + "authentication.digest.charset";
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET = DEFAULT_CONFIGURATION_PREFIX + HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET;
    public static final String CONFIG_DEFAULT_HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_CHARSET_DOC = "charset used to encode proxy 'digest' credentials. default is 'US-ASCII'";



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

                //follow redirect
                .define(prefix + OKHTTP_FOLLOW_REDIRECT, ConfigDef.Type.STRING, TRUE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_FOLLOW_REDIRECT_DOC)
                .define(prefix + OKHTTP_FOLLOW_SSL_REDIRECT, ConfigDef.Type.STRING, TRUE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_FOLLOW_SSL_REDIRECT_DOC)

                //interceptors
                .define(prefix + OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE, ConfigDef.Type.STRING, TRUE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE_DOC)
                .define(prefix + OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE_DOC)
                .define(prefix + OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE_DOC);

    }
}
