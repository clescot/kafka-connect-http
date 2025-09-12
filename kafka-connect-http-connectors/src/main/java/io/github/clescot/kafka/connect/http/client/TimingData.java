package io.github.clescot.kafka.connect.http.client;

public class TimingData {

    private long dnsDurationNs;
    private long connectDurationNs;
    private long requestHeadersDurationNs;
    private long responseHeadersDurationNs;
    private long totalDurationNs;
    private long proxySelectionDurationNs;


    public long getDnsDurationNs() {
        return dnsDurationNs;
    }

    public void setDnsDurationNs(long dnsDurationNs) {
        this.dnsDurationNs = dnsDurationNs;
    }

    public long getConnectDurationNs() {
        return connectDurationNs;
    }

    public void setConnectDurationNs(long connectDurationNs) {
        this.connectDurationNs = connectDurationNs;
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
}
