package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.nimbusds.oauth2.sdk.*;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpHTTPRequestSender;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;

public class OAuth2ClientCredentialsFlowAuthenticator implements Authenticator {

    private final OkHttpClient okHttpClient;
    private final ClientAuthentication clientAuth;
    private final URI tokenEndpointUri;
    private Scope scope;
    private AuthorizationGrant clientGrant = new ClientCredentialsGrant();

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2ClientCredentialsFlowAuthenticator.class);


    public OAuth2ClientCredentialsFlowAuthenticator(OkHttpClient okHttpClient,
                                                    String wellKnownUrl,
                                                    String clientId,
                                                    String clientSecret,
                                                    @javax.annotation.Nullable String... scopes) {
        this.okHttpClient = okHttpClient;
        ClientID clientID = new ClientID(clientId);
        Secret secret = new Secret(clientSecret);
        clientAuth = new ClientSecretBasic(clientID, secret);
        if (scopes != null && scopes.length > 0) {
            scope = new Scope(scopes);
        }
        //get oidc provider metadata
        Request.Builder builder = new Request.Builder();
        Request request = builder.url(wellKnownUrl).get().build();
        Response wellKnownResponse;
        try {
            wellKnownResponse = okHttpClient.newCall(request).execute();

            String wellKnownResponseBody = wellKnownResponse.body().string();

            // Read all data from URL
            String providerInfo;
            try (java.util.Scanner s = new java.util.Scanner(wellKnownResponseBody)) {
                providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
            }

            OIDCProviderMetadata providerMetadata;
            providerMetadata = OIDCProviderMetadata.parse(providerInfo);

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
                    throw new IllegalArgumentException("configured Scopes:'" + Joiner.on(",").join(configuredScopes) + "' are not all present in the scopes from the well known url ('" + Joiner.on(",").join(scopesFromWellKnownUrlList) + "')");
                }
            }
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    @Override
    public Request authenticate(@Nullable Route route, @NotNull Response response) throws IOException {
        final Request request = response.request();

        // Make the token request
        Tokens tokens;
        try {
            tokens = getTokens(tokenEndpointUri, clientAuth, clientGrant, scope);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        if (tokens != null) {
            AccessToken accessToken = tokens.getAccessToken();

            //no refresh token is issued for client credentials flow
            //cf RFC6749 section 4.4.3 https://www.rfc-editor.org/rfc/rfc6749#section-4.4.3

            // Get the access token as JSON string
            String accessTokenJSONString = accessToken.toJSONString();
            LOGGER.debug(accessTokenJSONString);
            String bearerToken = accessToken.toAuthorizationHeader();

            return request.newBuilder()
                    .header("Authorization", bearerToken)
                    .build();
        } else {
            LOGGER.error("no token has been issued");
        }
        return request;
    }


    private static final Interceptor interceptor = new Interceptor() {
        @NotNull
        @Override
        public Response intercept(@NotNull Chain chain) throws IOException {
            Request request = chain.request();

            long t1 = System.nanoTime();
            String log = String.format("Sending request %s on %s%n%s", request.url(), chain.connection(), request.headers());
            System.out.println(log);

            Response response = chain.proceed(request);

            long t2 = System.nanoTime();
            double elapsedTime = (t2 - t1) / 1e6d;
            String log2 = String.format("Received response for %s on %s%n%s in %.1fms%n%s %s%n%s",
                    response.request().url(), chain.connection(), request.headers(), elapsedTime, response.code(), response.message(), response.headers());
            System.out.println(log2);
            return response;
        }
    };


    private Tokens getTokens(URI tokenEndpointUri, ClientAuthentication clientAuth, AuthorizationGrant clientGrant, Scope scope) throws ParseException, IOException {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.addNetworkInterceptor(interceptor);
        OkHttpClient okHttpClient = builder.build();
        TokenRequest tokenRequest = new TokenRequest(tokenEndpointUri, clientAuth, clientGrant, scope);
        HTTPResponse httpResponse = tokenRequest.toHTTPRequest().send(new OkHttpHTTPRequestSender(okHttpClient));

        TokenResponse response = TokenResponse.parse(httpResponse);
        Tokens tokens = null;
        if (!response.indicatesSuccess()) {
            // We got an error response...
            TokenErrorResponse errorResponse = response.toErrorResponse();
            System.err.println("error: " + errorResponse.toJSONObject());
        } else {
            AccessTokenResponse successResponse = response.toSuccessResponse();
            tokens = successResponse.getTokens();
        }
        return tokens;
    }
}