package io.github.clescot.kafka.connect.http.client.oauth;

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
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * machine to machine scenario.
 */
//@ExtendWith(SpringExtension.class)
//@SpringBootTest(
//        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
//        classes = OAuth2LoginApp.class,
//        //these can be set in application yaml if you desire
//        properties = {
//                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-id=testclient",
//                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".client-secret=testsecret",
//                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".authorization-grant-type=client_credentials",
//                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".redirect-uri={baseUrl}/login/oauth2/code/{registrationId}",
//                OAuth2ClientCredentialsFlowLoginAppTest.REGISTRATION + PROVIDER_ID + ".scope=openid",
//                OAuth2ClientCredentialsFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".authorization-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/authorize",
//                OAuth2ClientCredentialsFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".token-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/token",
//                OAuth2ClientCredentialsFlowLoginAppTest.PROVIDER + PROVIDER_ID + ".jwk-set-uri=${" + MOCK_OAUTH_2_SERVER_BASE_URL + "}/issuer1/jwks"
//        }
//)
//client-authentication-method: client_secret_jwt client_secret_basic client_secret_post private_key_jwt none
//@ContextConfiguration(initializers = {MockOAuth2ServerInitializer.class})
public class OAuth2ClientCredentialsFlowLoginAppTest {
//    public static final String CLIENT = "spring.security.oauth2.client";
//    public static final String PROVIDER = CLIENT + ".provider.";
//    public static final String REGISTRATION = CLIENT + ".registration.";
//    public static final String PROVIDER_ID = "myprovider";
    public static final String OPENID = "openid";

    private static final  Interceptor interceptor = new Interceptor() {
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

    @Test
    public void test_spotify() throws IOException, ParseException {


        //get oidc provider metadata
        String wellKnownurl = "https://accounts.spotify.com/.well-known/openid-configuration";
        URL providerConfigurationURL = new URL(wellKnownurl);
        InputStream stream = providerConfigurationURL.openStream();
        // Read all data from URL
        String providerInfo;
        try (java.util.Scanner s = new java.util.Scanner(stream)) {
            providerInfo = s.useDelimiter("\\A").hasNext() ? s.next() : "";
        }
        OIDCProviderMetadata providerMetadata = OIDCProviderMetadata.parse(providerInfo);
        // The token endpoint
        URI tokenEndpointUri = providerMetadata.getTokenEndpointURI();
        String tokenEndpoint = tokenEndpointUri.toString();
        String issuer = tokenEndpoint.substring(0, tokenEndpoint.length() - "/token".length());

        //get access token with client credential flow

        // Construct the client credentials grant
        AuthorizationGrant clientGrant = new ClientCredentialsGrant();

        // The credentials to authenticate the client at the token endpoint
        ClientID clientID = new ClientID("");
        Secret clientSecret = new Secret("");
        ClientAuthentication clientAuth = new ClientSecretBasic(clientID, clientSecret);
        Scope scopes = providerMetadata.getScopes();
        List<String> scopesList = scopes.toStringList();
        // The request scope for the token (may be optional)
        assertThat(scopesList).contains(OPENID);
//        Scope scope = new Scope(OPENID,"email","profile");
        Scope scope = null;
//        Scope scope = new Scope("read", "write");


        // Make the token request
        Tokens tokens = getTokens(tokenEndpointUri, clientAuth, clientGrant, scope);
        if(tokens!=null) {
            AccessToken accessToken = tokens.getAccessToken();

            //no refresh token is issued for client credentials flow
            //cf RFC6749 section 4.4.3 https://www.rfc-editor.org/rfc/rfc6749#section-4.4.3

            // Get the access token as JSON string
            String accessTokenJSONString = accessToken.toJSONString();
            System.out.println(accessTokenJSONString);
            String bearerToken = accessToken.toAuthorizationHeader();



            OkHttpClient okHttpClient = new OkHttpClient.Builder()
                    .followRedirects(true)
                    .followSslRedirects(true)
                    .addNetworkInterceptor(interceptor)
                    .build();

            HttpUrl okHttpUrl = HttpUrl.parse("https://api.spotify.com/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V");
//            HttpUrl okHttpUrl = HttpUrl.parse("http://localhost:" + localPort + "/api/ping");
            okhttp3.Request request1 = new Request.Builder()
                    .url(okHttpUrl)
                    .header("Accept", "text/html")
                    .header("Authorization", bearerToken)
                    .method("GET", null).build();
            Response response1 = okHttpClient.newCall(request1).execute();
            assertThat(response1).isNotNull();
            String bodyString = response1.body().string();
            String expected = "{\n" +
                    "  \"album\" : {\n" +
                    "    \"album_type\" : \"album\",\n" +
                    "    \"artists\" : [ {\n" +
                    "      \"external_urls\" : {\n" +
                    "        \"spotify\" : \"https://open.spotify.com/artist/08td7MxkoHQkXnWAYD8d6Q\"\n" +
                    "      },\n" +
                    "      \"href\" : \"https://api.spotify.com/v1/artists/08td7MxkoHQkXnWAYD8d6Q\",\n" +
                    "      \"id\" : \"08td7MxkoHQkXnWAYD8d6Q\",\n" +
                    "      \"name\" : \"Tania Bowra\",\n" +
                    "      \"type\" : \"artist\",\n" +
                    "      \"uri\" : \"spotify:artist:08td7MxkoHQkXnWAYD8d6Q\"\n" +
                    "    } ],\n" +
                    "    \"available_markets\" : [ \"AR\", \"AU\", \"AT\", \"BE\", \"BO\", \"BR\", \"BG\", \"CA\", \"CL\", \"CO\", \"CR\", \"CY\", \"CZ\", \"DK\", \"DO\", \"DE\", \"EC\", \"EE\", \"SV\", \"FI\", \"FR\", \"GR\", \"GT\", \"HN\", \"HK\", \"HU\", \"IS\", \"IE\", \"IT\", \"LV\", \"LT\", \"LU\", \"MY\", \"MT\", \"MX\", \"NL\", \"NZ\", \"NI\", \"NO\", \"PA\", \"PY\", \"PE\", \"PH\", \"PL\", \"PT\", \"SG\", \"SK\", \"ES\", \"SE\", \"CH\", \"TW\", \"TR\", \"UY\", \"US\", \"GB\", \"AD\", \"LI\", \"MC\", \"ID\", \"JP\", \"TH\", \"VN\", \"RO\", \"IL\", \"ZA\", \"SA\", \"AE\", \"BH\", \"QA\", \"OM\", \"KW\", \"EG\", \"MA\", \"DZ\", \"TN\", \"LB\", \"JO\", \"PS\", \"IN\", \"BY\", \"KZ\", \"MD\", \"UA\", \"AL\", \"BA\", \"HR\", \"ME\", \"MK\", \"RS\", \"SI\", \"KR\", \"BD\", \"PK\", \"LK\", \"GH\", \"KE\", \"NG\", \"TZ\", \"UG\", \"AG\", \"AM\", \"BS\", \"BB\", \"BZ\", \"BT\", \"BW\", \"BF\", \"CV\", \"CW\", \"DM\", \"FJ\", \"GM\", \"GE\", \"GD\", \"GW\", \"GY\", \"HT\", \"JM\", \"KI\", \"LS\", \"LR\", \"MW\", \"MV\", \"ML\", \"MH\", \"FM\", \"NA\", \"NR\", \"NE\", \"PW\", \"PG\", \"PR\", \"WS\", \"SM\", \"ST\", \"SN\", \"SC\", \"SL\", \"SB\", \"KN\", \"LC\", \"VC\", \"SR\", \"TL\", \"TO\", \"TT\", \"TV\", \"VU\", \"AZ\", \"BN\", \"BI\", \"KH\", \"CM\", \"TD\", \"KM\", \"GQ\", \"SZ\", \"GA\", \"GN\", \"KG\", \"LA\", \"MO\", \"MR\", \"MN\", \"NP\", \"RW\", \"TG\", \"UZ\", \"ZW\", \"BJ\", \"MG\", \"MU\", \"MZ\", \"AO\", \"CI\", \"DJ\", \"ZM\", \"CD\", \"CG\", \"IQ\", \"LY\", \"TJ\", \"VE\", \"ET\", \"XK\" ],\n" +
                    "    \"external_urls\" : {\n" +
                    "      \"spotify\" : \"https://open.spotify.com/album/6akEvsycLGftJxYudPjmqK\"\n" +
                    "    },\n" +
                    "    \"href\" : \"https://api.spotify.com/v1/albums/6akEvsycLGftJxYudPjmqK\",\n" +
                    "    \"id\" : \"6akEvsycLGftJxYudPjmqK\",\n" +
                    "    \"images\" : [ {\n" +
                    "      \"height\" : 640,\n" +
                    "      \"url\" : \"https://i.scdn.co/image/ab67616d0000b2731ae2bdc1378da1b440e1f610\",\n" +
                    "      \"width\" : 640\n" +
                    "    }, {\n" +
                    "      \"height\" : 300,\n" +
                    "      \"url\" : \"https://i.scdn.co/image/ab67616d00001e021ae2bdc1378da1b440e1f610\",\n" +
                    "      \"width\" : 300\n" +
                    "    }, {\n" +
                    "      \"height\" : 64,\n" +
                    "      \"url\" : \"https://i.scdn.co/image/ab67616d000048511ae2bdc1378da1b440e1f610\",\n" +
                    "      \"width\" : 64\n" +
                    "    } ],\n" +
                    "    \"name\" : \"Place In The Sun\",\n" +
                    "    \"release_date\" : \"2004-02-02\",\n" +
                    "    \"release_date_precision\" : \"day\",\n" +
                    "    \"total_tracks\" : 11,\n" +
                    "    \"type\" : \"album\",\n" +
                    "    \"uri\" : \"spotify:album:6akEvsycLGftJxYudPjmqK\"\n" +
                    "  },\n" +
                    "  \"artists\" : [ {\n" +
                    "    \"external_urls\" : {\n" +
                    "      \"spotify\" : \"https://open.spotify.com/artist/08td7MxkoHQkXnWAYD8d6Q\"\n" +
                    "    },\n" +
                    "    \"href\" : \"https://api.spotify.com/v1/artists/08td7MxkoHQkXnWAYD8d6Q\",\n" +
                    "    \"id\" : \"08td7MxkoHQkXnWAYD8d6Q\",\n" +
                    "    \"name\" : \"Tania Bowra\",\n" +
                    "    \"type\" : \"artist\",\n" +
                    "    \"uri\" : \"spotify:artist:08td7MxkoHQkXnWAYD8d6Q\"\n" +
                    "  } ],\n" +
                    "  \"available_markets\" : [ \"AR\", \"AU\", \"AT\", \"BE\", \"BO\", \"BR\", \"BG\", \"CA\", \"CL\", \"CO\", \"CR\", \"CY\", \"CZ\", \"DK\", \"DO\", \"DE\", \"EC\", \"EE\", \"SV\", \"FI\", \"FR\", \"GR\", \"GT\", \"HN\", \"HK\", \"HU\", \"IS\", \"IE\", \"IT\", \"LV\", \"LT\", \"LU\", \"MY\", \"MT\", \"MX\", \"NL\", \"NZ\", \"NI\", \"NO\", \"PA\", \"PY\", \"PE\", \"PH\", \"PL\", \"PT\", \"SG\", \"SK\", \"ES\", \"SE\", \"CH\", \"TW\", \"TR\", \"UY\", \"US\", \"GB\", \"AD\", \"LI\", \"MC\", \"ID\", \"JP\", \"TH\", \"VN\", \"RO\", \"IL\", \"ZA\", \"SA\", \"AE\", \"BH\", \"QA\", \"OM\", \"KW\", \"EG\", \"MA\", \"DZ\", \"TN\", \"LB\", \"JO\", \"PS\", \"IN\", \"BY\", \"KZ\", \"MD\", \"UA\", \"AL\", \"BA\", \"HR\", \"ME\", \"MK\", \"RS\", \"SI\", \"KR\", \"BD\", \"PK\", \"LK\", \"GH\", \"KE\", \"NG\", \"TZ\", \"UG\", \"AG\", \"AM\", \"BS\", \"BB\", \"BZ\", \"BT\", \"BW\", \"BF\", \"CV\", \"CW\", \"DM\", \"FJ\", \"GM\", \"GE\", \"GD\", \"GW\", \"GY\", \"HT\", \"JM\", \"KI\", \"LS\", \"LR\", \"MW\", \"MV\", \"ML\", \"MH\", \"FM\", \"NA\", \"NR\", \"NE\", \"PW\", \"PG\", \"PR\", \"WS\", \"SM\", \"ST\", \"SN\", \"SC\", \"SL\", \"SB\", \"KN\", \"LC\", \"VC\", \"SR\", \"TL\", \"TO\", \"TT\", \"TV\", \"VU\", \"AZ\", \"BN\", \"BI\", \"KH\", \"CM\", \"TD\", \"KM\", \"GQ\", \"SZ\", \"GA\", \"GN\", \"KG\", \"LA\", \"MO\", \"MR\", \"MN\", \"NP\", \"RW\", \"TG\", \"UZ\", \"ZW\", \"BJ\", \"MG\", \"MU\", \"MZ\", \"AO\", \"CI\", \"DJ\", \"ZM\", \"CD\", \"CG\", \"IQ\", \"LY\", \"TJ\", \"VE\", \"ET\", \"XK\" ],\n" +
                    "  \"disc_number\" : 1,\n" +
                    "  \"duration_ms\" : 276773,\n" +
                    "  \"explicit\" : false,\n" +
                    "  \"external_ids\" : {\n" +
                    "    \"isrc\" : \"AUCR10410001\"\n" +
                    "  },\n" +
                    "  \"external_urls\" : {\n" +
                    "    \"spotify\" : \"https://open.spotify.com/track/2TpxZ7JUBn3uw46aR7qd6V\"\n" +
                    "  },\n" +
                    "  \"href\" : \"https://api.spotify.com/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V\",\n" +
                    "  \"id\" : \"2TpxZ7JUBn3uw46aR7qd6V\",\n" +
                    "  \"is_local\" : false,\n" +
                    "  \"name\" : \"All I Want\",\n" +
                    "  \"popularity\" : 2,\n" +
                    "  \"preview_url\" : \"https://p.scdn.co/mp3-preview/2cc385470731bc540a08caef5eab31dec7b036a6?cid=44d54a4d05344d97877d163209905f8b\",\n" +
                    "  \"track_number\" : 1,\n" +
                    "  \"type\" : \"track\",\n" +
                    "  \"uri\" : \"spotify:track:2TpxZ7JUBn3uw46aR7qd6V\"\n" +
                    "}";
            JSONAssert.assertEquals(expected,bodyString,true);
        }else{
            System.err.println("an error has been thrown");
        }
    }

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