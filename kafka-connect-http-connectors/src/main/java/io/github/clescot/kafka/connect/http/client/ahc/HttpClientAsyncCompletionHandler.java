package io.github.clescot.kafka.connect.http.client.ahc;

import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientAsyncCompletionHandler extends AsyncCompletionHandlerBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientAsyncCompletionHandler.class);



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
