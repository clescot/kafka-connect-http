package com.github.clescot.kafka.connect.http.sink.service;

import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WsCallerAsyncCompletionHandler extends AsyncCompletionHandlerBase {

    private final static Logger LOGGER = LoggerFactory.getLogger(WsCallerAsyncCompletionHandler.class);



    @Override
    public void onThrowable(Throwable t) {
        LOGGER.error(t.getMessage(), t);
        super.onThrowable(t);
    }

    @Override
    public State onStatusReceived(HttpResponseStatus status) throws Exception {
        LOGGER.debug("status response code={}, text={}",status.getStatusCode(),status.getStatusText());
        return super.onStatusReceived(status);
    }
}
