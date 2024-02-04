package io.github.clescot.kafka.connect.http.client.okhttp;

import dasniko.testcontainers.keycloak.KeycloakContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

@Testcontainers
public class OAuth2OkHttpClientTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2OkHttpClientTest.class);

    @Container
    private static final KeycloakContainer KEYCLOAK_CONTAINER = new KeycloakContainer();
//    private static final KeycloakContainer KEYCLOAK_CONTAINER = new KeycloakContainer().withRealmImportFile("/realm.json");

    @BeforeEach
    public void setUp(){
        String authServerUrl = KEYCLOAK_CONTAINER.getAuthServerUrl();
        String adminUsername = KEYCLOAK_CONTAINER.getAdminUsername();
        String adminPassword = KEYCLOAK_CONTAINER.getAdminPassword();
    }

    @AfterAll
    static void tearsDown(){
    }

    @Test
    void test_nominal() throws IOException {
        System.out.println(KEYCLOAK_CONTAINER.getHost()+":"+KEYCLOAK_CONTAINER.getHttpPort());
    }


}
