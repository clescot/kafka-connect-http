package com.github.clescot.kafka.connect.http.sink.client;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public interface HttpClient<Req, Res> {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    String BLANK_RESPONSE_CONTENT = "";
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
    Map<String, Pattern> httpSuccessCodesPatterns = Maps.newHashMap();


    /**
     * @param httpRequest
     * @return
     */

    default Pattern getSuccessPattern(HttpRequest httpRequest) {
        //by default, we don't resend any http call with a response between 100 and 499
        // 1xx is for protocol information (100 continue for example),
        // 2xx is for success,
        // 3xx is for redirection
        //4xx is for a client error
        //5xx is for a server error
        //only 5xx by default, trigger a resend

        /*
         *  HTTP Server status code returned
         *  3 cases can arise:
         *  * a success occurs : the status code returned from the ws server is matching the regexp => no retries
         *  * a functional error occurs: the status code returned from the ws server is not matching the regexp, but is lower than 500 => no retries
         *  * a technical error occurs from the WS server : the status code returned from the ws server does not match the regexp AND is equals or higher than 500 : retries are done
         */
        String wsSuccessCode = "^[1-4][0-9][0-9]$";

        if (this.httpSuccessCodesPatterns.get(wsSuccessCode) == null) {
            //Pattern.compile should be reused for performance, but wsSuccessCode can change....
            Pattern httpSuccessPattern = Pattern.compile(wsSuccessCode);
            httpSuccessCodesPatterns.put(wsSuccessCode, httpSuccessPattern);
        }
        return httpSuccessCodesPatterns.get(wsSuccessCode);
    }


    default HttpExchange getHttpExchange(HttpRequest httpRequest,
                                         HttpResponse httpResponse,
                                         Stopwatch stopwatch,
                                         OffsetDateTime now,
                                         AtomicInteger attempts) {
        return buildHttpExchange(
                httpRequest,
                httpResponse,
                stopwatch,
                now,
                attempts,
                SUCCESS
        );
    }


    default HttpExchange buildHttpExchange(HttpRequest httpRequest,
                                           HttpResponse httpResponse,
                                           Stopwatch stopwatch,
                                           OffsetDateTime now,
                                           AtomicInteger attempts,
                                           boolean success) {
        Preconditions.checkNotNull(httpRequest, "'httpRequest' is null");
        return HttpExchange.Builder.anHttpExchange()
                //request
                .withHttpRequest(httpRequest)
                //response
                .withHttpResponse(httpResponse)
                //technical metadata
                //time elapsed during http call
                .withDuration(stopwatch.elapsed(TimeUnit.MILLISECONDS))
                //at which moment occurs the beginning of the http call
                .at(now)
                .withAttempts(attempts)
                .withSuccess(success)
                .build();
    }



    <Res> Res buildRequest(HttpRequest httpRequest);

    HttpExchange call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException;
}
