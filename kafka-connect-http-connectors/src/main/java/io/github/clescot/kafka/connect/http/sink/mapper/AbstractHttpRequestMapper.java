package io.github.clescot.kafka.connect.http.sink.mapper;

import java.util.regex.Pattern;

public abstract class AbstractHttpRequestMapper implements HttpRequestMapper{
    protected Pattern splitPattern;
    protected  int splitLimit;
    @Override
    public void setSplitLimit(int splitLimit) {
        this.splitLimit = splitLimit;
    }
    @Override
    public void setSplitPattern(String splitPattern) {
        this.splitPattern = Pattern.compile(splitPattern);
    }
    @Override
    public int getSplitLimit() {
        return splitLimit;
    }
    @Override
    public Pattern getSplitPattern() {
        return splitPattern;
    }
}
