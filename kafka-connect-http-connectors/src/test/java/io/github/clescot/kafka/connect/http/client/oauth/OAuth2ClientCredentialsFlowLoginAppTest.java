package io.github.clescot.kafka.connect.http.client.oauth;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
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
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * machine to machine scenario.
 */

public class OAuth2ClientCredentialsFlowLoginAppTest {
    @RegisterExtension
    static WireMockExtension wmHttp;

    public static final String WELL_KNOWN_OK = "WellKnownOk";
    public static final String TOKEN_OK = "TokenOk";
    public static final String SONG_OK = "SongOk";

    static {

        wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                                .dynamicPort()
                                .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())
                                .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                )
                .build();
    }




    @Test
    public void test_spotify() throws IOException, ParseException {

        String scenario = "test_successful_request_at_first_time";
        WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
        WireMock wireMock = wmRuntimeInfo.getWireMock();
        Path path = Paths.get("src/test/resources/oauth2/wellknownUrlContent.json");
        String httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
        String content = Files.readString(path);
        String wellKnownUrlContent = content.replaceAll("baseUrl",httpBaseUrl);

        wireMock
                .register(WireMock.get("/.well-known/openid-configuration").inScenario(scenario)
                        .whenScenarioStateIs(STARTED)
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)
                                .withStatusMessage("OK")
                                .withBody(wellKnownUrlContent)
                        ).willSetStateTo(WELL_KNOWN_OK)
                );



        Path tokenPath = Paths.get("src/test/resources/oauth2/token.json");
        String tokenContent = Files.readString(tokenPath);
        wireMock
                .register(
                            WireMock.post("/api/token")
                                    .withHeader("Content-Type",containing("application/x-www-form-urlencoded; charset=UTF-8"))
                                    .withHeader("Authorization",containing("NDRkMzRhNGQwNTM0NGM5NzgzN2Q0NjMyMDc4MDVmOGI6M2ZjMDU3NjcyMDU0NGFjMjkzYTNhNTMwNGU2YzBmYTg="))
                        .inScenario(scenario)
                        .whenScenarioStateIs(WELL_KNOWN_OK)
                        .willReturn(WireMock.aResponse()
                                .withStatus(200)
                                .withStatusMessage("OK")
                                .withBody(tokenContent)
                        ).willSetStateTo(TOKEN_OK)
                );


        Path songPath = Paths.get("src/test/resources/oauth2/song.json");
        String songContent = Files.readString(songPath);
        wireMock
                .register(
                        WireMock.get("/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V")
                                .withHeader("Authorization",containing("Bearer BQDzs98uhifaGayk8H9tCTRozufhFmgV_HKMCnnDdMTdz1FcOo3sdj8OZJ_azo96LRdLI9_1uJOCXxbGZme11KCb6ZxTuCt8B5FxEeECb1kO_-UDuf8"))
                                .inScenario(scenario)
                                .whenScenarioStateIs(TOKEN_OK)
                                .willReturn(WireMock.aResponse()
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withBody(songContent)
                                ).willSetStateTo(SONG_OK)
                );



        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .followRedirects(true)
                .followSslRedirects(true)
                .build();
        Request.Builder builder = new Request.Builder();
        //get oidc provider metadata

        String wellKnownUrl = httpBaseUrl +"/.well-known/openid-configuration";
        Response wellKnownResponse;
        Request request = builder
                .url(wellKnownUrl)
                .get()
                .addHeader("Content-Type","application/json; charset=utf-8")
                .build();
        // Read all data from URL
        String providerInfo;
        wellKnownResponse = okHttpClient.newCall(request).execute();

        String wellKnownResponseBody = wellKnownResponse.body().string();
        try (java.util.Scanner s = new java.util.Scanner(wellKnownResponseBody)) {
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
        ClientID clientID = new ClientID("44d34a4d05344c97837d463207805f8b");
        Secret clientSecret = new Secret("3fc0576720544ac293a3a5304e6c0fa8");
        ClientAuthentication clientAuth = new ClientSecretBasic(clientID, clientSecret);
        Scope scopes = providerMetadata.getScopes();
        List<String> scopesList = scopes.toStringList();
        // The request scope for the token (may be optional)
        assertThat(scopesList).contains("openid");
        Scope scope = null;


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





            HttpUrl okHttpUrl = HttpUrl.parse(httpBaseUrl+"/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V");
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
            System.err.println("no token has been generated");
        }
    }

    private Tokens getTokens(URI tokenEndpointUri, ClientAuthentication clientAuth, AuthorizationGrant clientGrant, Scope scope) throws ParseException, IOException {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
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