package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.*;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpHTTPRequestSender;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class OAuth2ClientCredentialsFlowAuthenticator implements CachingAuthenticator {

    private ClientAuthentication clientAuth;
    private final URI tokenEndpointUri;
    private final OkHttpClient okHttpClient;
    private Scope scope;
    private static final AuthorizationGrant CLIENT_CREDENTIALS_GRANT = new ClientCredentialsGrant();

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2ClientCredentialsFlowAuthenticator.class);
    private String bearerToken;

    public OAuth2ClientCredentialsFlowAuthenticator(OkHttpClient okHttpClient,
                                                    String wellKnownUrl,
                                                    Map<String, Object> config,
                                                    @javax.annotation.Nullable String... scopes) {
        this.okHttpClient = okHttpClient;
        Preconditions.checkNotNull(okHttpClient, "okHttpClient is null");
        Preconditions.checkNotNull(wellKnownUrl, "wellKnownUrl is null");

        if (scopes != null && scopes.length > 0) {
            scope = new Scope(scopes);
        }
        //get oidc provider metadata
        Request.Builder builder = new Request.Builder();
        Request request = builder.url(wellKnownUrl).get().build();
        Response wellKnownResponse;
        String wellKnownResponseBody = null;
        try {
            wellKnownResponse = okHttpClient.newCall(request).execute();
            wellKnownResponseBody = wellKnownResponse.body().string();

            // Read all data from URL
            String providerInfo;
            try (Scanner s = new Scanner(wellKnownResponseBody)) {
                providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
            }

            OIDCProviderMetadata providerMetadata;
            providerMetadata = OIDCProviderMetadata.parse(providerInfo);

            List<ClientAuthenticationMethod> tokenEndpointAuthMethods = providerMetadata.getTokenEndpointAuthMethods();
            tokenEndpointUri = providerMetadata.getTokenEndpointURI();
            clientAuth = buildClientAuthentication(config,tokenEndpointUri);

            if (!tokenEndpointAuthMethods.contains(clientAuth.getMethod())) {
                throw new IllegalStateException("Oauth2 provider does not support '" + clientAuth.getMethod().getValue() + "' authentication to get the token");
            }
            // The token endpoint

            String tokenEndpoint = tokenEndpointUri.toString();
            String issuer = tokenEndpoint.substring(0, tokenEndpoint.length() - "/token".length());




            //get access token with client credential flow


            // The credentials to authenticate the client at the token endpoint

            Scope scopesFromWellKnownUrl = providerMetadata.getScopes();
            Set<String> scopesFromWellKnownUrlList = Sets.newHashSet(scopesFromWellKnownUrl.toStringList());
            if (scopes != null && scopes.length > 0) {
                Set<String> configuredScopes = Sets.newHashSet(Arrays.asList(scopes));
                boolean configuredScopesAreValid = scopesFromWellKnownUrlList.containsAll(configuredScopes);
                if (!configuredScopesAreValid) {
                    throw new IllegalArgumentException("configured Scopes:'"
                            + Joiner.on(",").join(configuredScopes) + "' are not all present in the scopes from the well known url ('"
                            + Joiner.on(",").join(scopesFromWellKnownUrlList) + "')");
                }
            }
        } catch (IOException | ParseException e) {
            LOGGER.error("error in parsing wellKnown Url content:'{}'", wellKnownResponseBody);
            throw new IllegalStateException(e);
        }

    }


    private ClientAuthentication buildClientAuthentication(Map<String, Object> config, URI tokenEndpointUri) {

        Object clientAuthenticationmethod = Optional.ofNullable(config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_AUTHENTICATION_METHOD)).orElse("client_secret_basic");
        ClientAuthenticationMethod clientAuthenticationMethod = ClientAuthenticationMethod.parse(clientAuthenticationmethod.toString());
        String clientAuthenticationMethodValue = clientAuthenticationMethod.getValue();
        LOGGER.debug("clientAuthenticationMethod:'{}'", clientAuthenticationMethodValue);
        ClientAuthentication clientAuth;
        switch (clientAuthenticationMethodValue) {
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
            case "client_secret_jwt": {
                ClientID clientID = new ClientID(getClientId(config));

                Object issuerObject = config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ISSUER);
                String issuerAsString;
                Issuer issuer;
                if (issuerObject != null && !issuerObject.toString().isEmpty()) {
                    issuerAsString = issuerObject.toString();
                    issuer = new Issuer(issuerAsString);
                } else {
                    issuer = new Issuer(clientID);
                }

                Secret secret = new Secret(getClientSecret(config));

                try {
                    Object jwsAlgorithmObject = Optional.ofNullable(config.get(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_JWS_ALGORITHM)).orElse("HS256");
                    String jwsAlgorithmName = jwsAlgorithmObject.toString();
                    JWSAlgorithm jwsAlgorithm = new JWSAlgorithm(jwsAlgorithmName);

                    clientAuth = new ClientSecretJWT(issuer, clientID, tokenEndpointUri, jwsAlgorithm, secret);
                } catch (JOSEException e) {
                    throw new RuntimeException(e);
                }
                break;
            }
            case "none":
            case "private_key_jwt":  //TODO clientAuth = new PrivateKeyJWT();
            case "tls_client_auth": //TODO clientAuth = new PKITLSClientAuthentication();
            case "self_signed_tls_client_auth"://TODO clientAuth = new SelfSignedTLSClientAuthentication();
            case "request_object": // TODO ??
            default:
                throw new IllegalArgumentException(clientAuthenticationMethodValue + " not supported");
        }


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


    @Nullable
    @Override
    public Request authenticate(@Nullable Route route, @NotNull Response response) throws IOException {
        final Request request = response.request();

        // Make the token request
        Tokens tokens;
        try {
            tokens = getTokens(tokenEndpointUri, clientAuth, CLIENT_CREDENTIALS_GRANT, scope);
        } catch (ParseException e) {
            LOGGER.error("tokencontent cannot be parsed");
            return request;
        }
        if (tokens != null) {
            AccessToken accessToken = tokens.getAccessToken();

            //no refresh token is issued for client credentials flow
            //cf RFC6749 section 4.4.3 https://www.rfc-editor.org/rfc/rfc6749#section-4.4.3

            // Get the access token as JSON string
            String accessTokenJSONString = accessToken.toJSONString();
            LOGGER.debug(accessTokenJSONString);
            bearerToken = accessToken.toAuthorizationHeader();

            return request.newBuilder()
                    .header("Authorization", bearerToken)
                    .build();
        } else {
            LOGGER.error("no token has been issued");
            return request;
        }
    }

    private Tokens getTokens(URI tokenEndpointUri, ClientAuthentication clientAuth, AuthorizationGrant clientGrant, Scope scope) throws ParseException, IOException {
        TokenRequest tokenRequest = new TokenRequest(tokenEndpointUri, clientAuth, clientGrant, scope);
        HTTPResponse httpResponse = tokenRequest.toHTTPRequest().send(new OkHttpHTTPRequestSender(okHttpClient));

        TokenResponse response = TokenResponse.parse(httpResponse);
        Tokens tokens = null;
        if (!response.indicatesSuccess()) {
            // We got an error response...
            TokenErrorResponse errorResponse = response.toErrorResponse();
            LOGGER.error("error:'{}'", errorResponse.toJSONObject());
        } else {
            AccessTokenResponse successResponse = response.toSuccessResponse();
            tokens = successResponse.getTokens();
        }
        return tokens;
    }

    @Override
    public Request authenticateWithState(Route route, Request request) {
        return request.newBuilder()
                .header("Authorization", bearerToken)
                .build();
    }
}
