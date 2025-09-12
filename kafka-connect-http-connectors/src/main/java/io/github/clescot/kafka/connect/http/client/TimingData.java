package io.github.clescot.kafka.connect.http.client;

public class TimingData {

    private long dnsDurationNs;
    private long connectingDurationNs;
    private long connectedDurationNs;
    private long requestHeadersDurationNs;
    private long responseHeadersDurationNs;
    private long totalDurationNs;
    private long proxySelectionDurationNs;
    private long requestBodyDurationNs;
    private long responseBodyDurationNs;
    private long secureConnectingDurationNs;


    public long getDnsDurationNs() {
        return dnsDurationNs;
    }

    public void setDnsDurationNs(long dnsDurationNs) {
        this.dnsDurationNs = dnsDurationNs;
    }

    public long getConnectingDurationNs() {
        return connectingDurationNs;
    }

    public void setConnectingDurationNs(long connectingDurationNs) {
        this.connectingDurationNs = connectingDurationNs;
    }

    public long getConnectedDurationNs() {
        return connectedDurationNs;
    }

    public void setConnectedDurationNs(long connectedDurationNs) {
        this.connectedDurationNs = connectedDurationNs;
    }

    public void setProxySelectionDurationNs(long proxySelectionDurationNs) {
        this.proxySelectionDurationNs = proxySelectionDurationNs;
    }

    public long getRequestHeadersDurationNs() {
        return requestHeadersDurationNs;
    }

    public void setRequestHeadersDurationNs(long requestHeadersDurationNs) {
        this.requestHeadersDurationNs = requestHeadersDurationNs;
    }

    public long getResponseHeadersDurationNs() {
        return responseHeadersDurationNs;
    }

    public void setResponseHeadersDurationNs(long responseHeadersDurationNs) {
        this.responseHeadersDurationNs = responseHeadersDurationNs;
    }

    public long getTotalDurationNs() {
        return totalDurationNs;
    }

    public void setTotalDurationNs(long totalDurationNs) {
        this.totalDurationNs = totalDurationNs;
    }

    public void setProxySelectDurationNs(long duration) {
        this.proxySelectionDurationNs = duration;
    }

    public long getProxySelectionDurationNs() {
        return proxySelectionDurationNs;
    }

    public void setRequestBodyDurationNs(long duration) {
        this.requestBodyDurationNs = duration;
    }

    public long getRequestBodyDurationNs() {
        return requestBodyDurationNs;
    }

    public void setResponseBodyDurationNs(long duration) {
        this.responseBodyDurationNs = duration;
    }

    public long getResponseBodyDurationNs() {
        return responseBodyDurationNs;
    }

    public void setSecureConnectingDurationNs(long duration) {
        this.secureConnectingDurationNs = duration;
    }

    public long getSecureConnectingDurationNs() {
        return secureConnectingDurationNs;
    }
}
