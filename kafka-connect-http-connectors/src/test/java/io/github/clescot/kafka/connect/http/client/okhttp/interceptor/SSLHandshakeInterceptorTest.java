package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.client.DummyX509Certificate;
import okhttp3.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SSLHandshakeInterceptorTest {
    private final Logger LOGGER = LoggerFactory.getLogger(SSLHandshakeInterceptorTest.class);
    @Mock
    Interceptor.Chain chain;
    @Mock
    Request request;
    @Mock
    Response response;
    @Mock
    Connection connection;
    @Mock
    Handshake handshake;
    @Mock
    CipherSuite cipherSuite;
    @Mock
    Principal peerPrincipal;
    @Mock
    Principal localPrincipal;
    @Test
    void test_intercept() throws IOException {
        //given
        when(chain.request()).thenReturn(request);
        when(handshake.localPrincipal()).thenReturn(localPrincipal);
        List<Certificate> localCertificates = Lists.newArrayList();
        DummyX509Certificate localCertificate = new DummyX509Certificate();
        X500Principal localPrincipal = new X500Principal("CN=Duke, OU=JavaSoft, O=Sun Microsystems, C=US");
        localCertificate.setSubjectDN(localPrincipal);
        localCertificates.add(localCertificate);
        when(handshake.localCertificates()).thenReturn(localCertificates);
        when(this.localPrincipal.getName()).thenReturn("CN=dummy.com,O=My Corp.inc,L=Boston,ST=Massachusetts,C=US");
        when(handshake.peerPrincipal()).thenReturn(peerPrincipal);
        when(peerPrincipal.getName()).thenReturn("CN=yahoo.com,O=Oath Holdings Inc.,L=Sunnyvale,ST=California,C=US");
        List<Certificate> peerCertificates = Lists.newArrayList();
        DummyX509Certificate peerCertificate = new DummyX509Certificate();
        X500Principal peerPrincipal = new X500Principal("CN=Remote, OU=stuff, O=Remote Corp, C=US");
        peerCertificate.setSubjectDN(peerPrincipal);
        peerCertificates.add(peerCertificate);
        when(handshake.peerCertificates()).thenReturn(peerCertificates);
        when(handshake.cipherSuite()).thenReturn(cipherSuite);
        when(cipherSuite.javaName()).thenReturn("TLS_AES_128_GCM_SHA256");
        when(connection.handshake()).thenReturn(handshake);
        when(chain.connection()).thenReturn(connection);
        when(chain.proceed(request)).thenReturn(response);
        SSLHandshakeInterceptor sslHandshakeInterceptor = new SSLHandshakeInterceptor();

        //when
        Response myResponse = sslHandshakeInterceptor.intercept(chain);
        //then
        assertThat(myResponse).isNotNull();
    }
}