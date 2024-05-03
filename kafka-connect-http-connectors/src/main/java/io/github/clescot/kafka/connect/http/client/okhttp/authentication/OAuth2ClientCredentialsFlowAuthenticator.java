package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
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
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_BASIC;

public class OAuth2ClientCredentialsFlowAuthenticator implements CachingAuthenticator {

    private final ClientAuthentication clientAuth;
    private final URI tokenEndpointUri;
    private final OkHttpClient okHttpClient;
    private Scope scope;
    private static final AuthorizationGrant CLIENT_CREDENTIALS_GRANT = new ClientCredentialsGrant();

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2ClientCredentialsFlowAuthenticator.class);
    private String bearerToken;

    public OAuth2ClientCredentialsFlowAuthenticator(OkHttpClient okHttpClient,
                                                    String wellKnownUrl,
                                                    String clientId,
                                                    String clientSecret,
                                                    @javax.annotation.Nullable String... scopes) {
        this.okHttpClient = okHttpClient;
        Preconditions.checkNotNull(okHttpClient,"okHttpClient is null");
        Preconditions.checkNotNull(wellKnownUrl,"wellKnownUrl is null");
        Preconditions.checkNotNull(clientId,"clientId is null");
        Preconditions.checkNotNull(clientSecret,"clientSecret is null");

//        Client Authentication Methods
//
//        Depending on the authorization server configuration, client applications can use one of the following authentication methods:
//
//        1- Client secret based authentication:
//
//        * client_secret_basic
//
//        * client_secret_post
//
//        * client_secret_jwt
//
//        2- JSON Web Token based authentication:
//
//        * private_key_jwt
//
//        3- Mutual Transport Layer Security (mTLS) based authentication:
//
//        * tls_auth
//
//        * self_signed_tls_client_auth
//
//        4- none authentication that elevates the use of Proof Key for Code Exchange (PKCE)



        //Basic Authentication
        ClientID clientID = new ClientID(clientId);
        Secret secret = new Secret(clientSecret);
        clientAuth = new ClientSecretBasic(clientID, secret);

        //Form Authentication
        //clientAuth = new ClientSecretPost(clientID,secret);

        //JWT token authentication
        //clientAuth = new ClientSecretJWT();
        //clientAuth = new PKITLSClientAuthentication();
        //clientAuth = new PrivateKeyJWT();
        //clientAuth = new SelfSignedTLSClientAuthentication();
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

            if(!tokenEndpointAuthMethods.contains(CLIENT_SECRET_BASIC)){
                throw new IllegalStateException("Oauth2 provider does not support 'client_secret_basic' authentication to get the token");
            }
            // The token endpoint
            tokenEndpointUri = providerMetadata.getTokenEndpointURI();
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
            LOGGER.error("error in parsing wellKnown Url content:'{}'",wellKnownResponseBody);
            throw new IllegalStateException(e);
        }
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
            LOGGER.error("error:'{}'",errorResponse.toJSONObject());
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
