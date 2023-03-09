package io.github.clescot.kafka.connect.http.sink.client.ahc;

import io.netty.channel.Channel;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.Response;
import org.asynchttpclient.netty.request.NettyRequest;

import javax.net.ssl.SSLSession;
import java.net.InetSocketAddress;
import java.util.List;

public class DummyAsyncHandler extends AsyncCompletionHandler {
    @Override
    public Object onCompleted(Response response) throws Exception {
        return null;
    }

    @Override
    public void onHostnameResolutionAttempt(String name) {
        super.onHostnameResolutionAttempt(name);
    }

    @Override
    public void onHostnameResolutionFailure(String name, Throwable cause) {
        super.onHostnameResolutionFailure(name, cause);
    }

    @Override
    public void onTcpConnectAttempt(InetSocketAddress remoteAddress) {
        super.onTcpConnectAttempt(remoteAddress);
    }

    @Override
    public void onTcpConnectSuccess(InetSocketAddress remoteAddress, Channel connection) {
        super.onTcpConnectSuccess(remoteAddress, connection);
    }

    @Override
    public void onTcpConnectFailure(InetSocketAddress remoteAddress, Throwable cause) {
        super.onTcpConnectFailure(remoteAddress, cause);
    }

    @Override
    public void onTlsHandshakeAttempt() {
        super.onTlsHandshakeAttempt();
    }

    @Override
    public void onTlsHandshakeSuccess(SSLSession sslSession) {
        super.onTlsHandshakeSuccess(sslSession);
    }

    @Override
    public void onTlsHandshakeFailure(Throwable cause) {
        super.onTlsHandshakeFailure(cause);
    }

    @Override
    public void onConnectionPoolAttempt() {
        super.onConnectionPoolAttempt();
    }

    @Override
    public void onConnectionPooled(Channel connection) {
        super.onConnectionPooled(connection);
    }

    @Override
    public void onConnectionOffer(Channel connection) {
        super.onConnectionOffer(connection);
    }

    @Override
    public void onRequestSend(NettyRequest request) {
        super.onRequestSend(request);
    }

    @Override
    public void onRetry() {
        super.onRetry();
    }

    @Override
    public void onHostnameResolutionSuccess(String name, List list) {
        super.onHostnameResolutionSuccess(name, list);
    }
}
