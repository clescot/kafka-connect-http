package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RequestSplitter {

    private final String id;
    private final Predicate<HttpRequest> predicate;
    private final String splitPattern;
    private final int splitLimit;

    public RequestSplitter(String id,
                           Predicate<HttpRequest> predicate,
                           String splitPattern,
                           int splitLimit) {
        this.id = id;
        this.predicate = predicate;
        this.splitPattern = splitPattern;
        this.splitLimit = splitLimit;
    }

    public String getId() {
        return id;
    }

    public int getSplitLimit() {
        return splitLimit;
    }

    public String getSplitPattern() {
        return splitPattern;
    }

    public boolean matches(HttpRequest httpRequest) {
        return this.predicate.test(httpRequest);
    }

    public List<String> split(String body){
        String pattern = getSplitPattern();
        List<String> parts = Lists.newArrayList();
        if (pattern != null && body!=null && !body.isBlank()) {
            parts = Lists.newArrayList(body.split(pattern, getSplitLimit()));
        } else {
            //no splitter
            parts.add(body);
        }
        return parts;
    }

    public List<Pair<SinkRecord, HttpRequest>> split(Pair<SinkRecord, HttpRequest> pair){
        List<HttpRequest> httpRequests = Lists.newArrayList();
        SinkRecord sinkRecord = pair.getLeft();
        HttpRequest httpRequest = pair.getRight();
        String bodyAsString = httpRequest.getBodyAsString();
        if (HttpRequest.BodyType.STRING.equals(httpRequest.getBodyType())
                 && bodyAsString!=null && !bodyAsString.isBlank()) {
            List<String> parts = split(bodyAsString);
            httpRequests.addAll(parts.stream()
                    .map(part -> {
                        HttpRequest partRequest = new HttpRequest(httpRequest);
                        partRequest.setBodyAsString(part);
                        return partRequest;
                    })
                    .collect(Collectors.toList()));
        } else {
            //no splitter
            httpRequests.add(httpRequest);
        }
        return httpRequests.stream().map(request-> Pair.of(sinkRecord,request)).collect(Collectors.toList());
    }
}
