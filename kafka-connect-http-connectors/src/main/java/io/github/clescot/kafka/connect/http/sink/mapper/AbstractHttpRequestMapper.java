package io.github.clescot.kafka.connect.http.sink.mapper;

import com.google.common.base.Preconditions;

public abstract class AbstractHttpRequestMapper implements HttpRequestMapper{
    protected String id;

    public AbstractHttpRequestMapper(String id) {
        Preconditions.checkNotNull(id);
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

}
