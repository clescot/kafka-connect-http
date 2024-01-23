package io.github.clescot.kafka.connect.http.client.okhttp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import okhttp3.OkHttpClient;
import okhttp3.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class OAuth2OkHttpClientTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2OkHttpClientTest.class);
    private static MockOAuth2Server server;

    @BeforeAll
    static void setUp(){
        server = new MockOAuth2Server();
        server.start();
    }

    @AfterAll
    static void tearsDown(){
        server.shutdown();
    }

    @Test
    void test_nominal() throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        String issuerId= "default";
        String wellKnownUrl = server.wellKnownUrl(issuerId).toString();
        Request request = new Request.Builder()
                .url(wellKnownUrl)
                .build();
        Call call = okHttpClient.newCall(request);
        Response response = call.execute();
        String wellKnownContent = response.body().string();
        LOGGER.info(wellKnownContent);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(wellKnownContent.getBytes(StandardCharsets.UTF_8));
        String authorizationEndpoint = jsonNode.get("authorization_endpoint").asText();
        LOGGER.info("authorizationEndpoint:{}",authorizationEndpoint);


        // Create the request body
        MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
        // Create the request body
        RequestBody body = RequestBody.create(mediaType, "response_type=token&client_id=ClientId&username=user&client_secret=userpassword&grant_type=client_credentials");
        // Build the request object, with method, headers
        Request request2 = new Request.Builder()
                .url("https://oauth2.url/oauth/token")
                .method("POST", body)
//                .addHeader("Authorization", createAuthHeaderString("ClientId", "Clientaccesskey"))
                .addHeader("Content-Type", "text/plain")
                .build();
        // Perform the request, this potentially throws an IOException
        Response response2 = okHttpClient.newCall(request2).execute();
        // Read the body of the response into a hashmap
        Map responseMap = objectMapper.
                readValue(response.body().byteStream(), HashMap.class);
        // Read the value of the "access_token" key from the hashmap
        String accessToken = (String)responseMap.get("access_token");
        // Return the access_token value






        // Build the request object, with method, headers
        Request request3 = new Request.Builder()
                .url(authorizationEndpoint)
                .method("POST", body)
                .build();


        Call call2 = okHttpClient.newCall(request2);
        Response response3 = call2.execute();
        String response2AsString = response2.body().string();
        LOGGER.info("response2AsString:{}",response2AsString);
    }


}
