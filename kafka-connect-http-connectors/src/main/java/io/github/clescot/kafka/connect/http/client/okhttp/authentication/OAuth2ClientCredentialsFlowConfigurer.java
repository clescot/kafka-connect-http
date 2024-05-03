package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.google.common.base.Preconditions;
import com.nimbusds.oauth2.sdk.auth.*;
import com.nimbusds.oauth2.sdk.id.ClientID;
import okhttp3.Authenticator;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class OAuth2ClientCredentialsFlowConfigurer implements AuthenticationConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2ClientCredentialsFlowConfigurer.class);
    private final OkHttpClient okHttpClient;

    public OAuth2ClientCredentialsFlowConfigurer(OkHttpClient okHttpClient) {
        Preconditions.checkNotNull(okHttpClient, "okHttp is null");
        this.okHttpClient = okHttpClient;
    }

    @Override
    public String authenticationScheme() {
        return "Bearer";
    }

    @Override
    public boolean needCache() {
        return true;
    }

    @Override
    public Authenticator configureAuthenticator(Map<String, Object> config) {
        Authenticator authenticator = null;
        Preconditions.checkNotNull(config, "config map is null");
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE)
                && config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE) != null
                && Boolean.TRUE.equals(Boolean.parseBoolean(config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE).toString()))) {

            Object wellKnownObject = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL);
            Preconditions.checkNotNull(wellKnownObject, HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL + " is null");
            String wellKnownUrl = wellKnownObject.toString();


            Object configuredScopes = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_SCOPES);
            String[] scopes = null;
            if (configuredScopes != null) {
                scopes = configuredScopes.toString().split(",");
            }
           ClientAuthentication clientAuth = buildClientAuthentication(config);
            authenticator = new OAuth2ClientCredentialsFlowAuthenticator(okHttpClient, wellKnownUrl, clientAuth, scopes);
        }
        return authenticator;
    }


    private ClientAuthentication buildClientAuthentication(Map<String, Object> config) {

        Object clientAuthenticationmethod = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD);
        Preconditions.checkNotNull(clientAuthenticationmethod,HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD+" parameter is required");
        ClientAuthenticationMethod clientAuthenticationMethod = ClientAuthenticationMethod.parse(clientAuthenticationmethod.toString());
        String clientAuthenticationMethodValue = clientAuthenticationMethod.getValue();
        LOGGER.debug("clientAuthenticationMethod:'{}'",clientAuthenticationMethodValue);
        ClientAuthentication clientAuth = null;
        switch(clientAuthenticationMethodValue){
            case "client_secret_basic": {
                ClientID clientID = new ClientID(getClientId(config));
                Secret secret = new Secret(getClientSecret(config));
                clientAuth = new ClientSecretBasic(clientID, secret);
                break;
            }
            case "client_secret_post": {
                ClientID clientID = new ClientID(getClientId(config));
                Secret secret = new Secret(getClientSecret(config));
                clientAuth = new ClientSecretPost(clientID, secret);
                break;
            }
            case "none":
            case "client_secret_jwt":
            case "private_key_jwt":
            case "tls_client_auth":
            case "self_signed_tls_client_auth":
            case "request_object":
            default:
                throw new IllegalArgumentException(clientAuthenticationMethodValue+" not supported");
        }

        //token_endpoint_auth_method parameter ?
        //token_endpoint_auth_methods_supported
        //0 => none?
        //1


        //JWT token authentication
        //clientAuth = new ClientSecretJWT();
        //clientAuth = new PKITLSClientAuthentication();
        //2
        //clientAuth = new PrivateKeyJWT();
        //3
        //clientAuth = new PKITLSClientAuthentication();
        //clientAuth = new SelfSignedTLSClientAuthentication();

        return clientAuth;
    }

    private String getClientId(Map<String, Object> config) {
        Object clientIdObject = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID);
        Preconditions.checkNotNull(clientIdObject, HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID + " is null");
        return clientIdObject.toString();
    }
    private String getClientSecret(Map<String, Object> config) {
        Object clientSecretObject = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET);
        Preconditions.checkNotNull(clientSecretObject, HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET + " is null");
        return clientSecretObject.toString();
    }


}
