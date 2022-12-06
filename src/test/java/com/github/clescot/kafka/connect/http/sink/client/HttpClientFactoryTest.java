package com.github.clescot.kafka.connect.http.sink.client;

import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManagerFactory;

import static com.github.clescot.kafka.connect.http.sink.HttpSinkTaskTest.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class HttpClientFactoryTest {


    @Test
    public void test_getTrustManagerFactory_jks_nominal_case(){

        //given
        String truststorePath = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
        String password = CLIENT_TRUSTSTORE_JKS_PASSWORD;
        //when
        TrustManagerFactory trustManagerFactory = HttpClientFactory.getTrustManagerFactory(truststorePath, password.toCharArray(), JKS_STORE_TYPE, TRUSTSTORE_PKIX_ALGORITHM);
        //then
        assertThat(trustManagerFactory).isNotNull();
        assertThat(trustManagerFactory.getTrustManagers()).hasSize(1);

    }


}