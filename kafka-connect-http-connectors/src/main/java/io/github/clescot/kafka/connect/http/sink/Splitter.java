package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Splitter {

    private final String id;
    private final Predicate<HttpRequest> predicate;
    private final String splitPattern;
    private final int splitLimit;

    public Splitter(String id,
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

    public List<Pair<SinkRecord, HttpRequest>> split(Pair<SinkRecord, HttpRequest> pair){
        List<Pair<SinkRecord, HttpRequest>> httpRequests = Lists.newArrayList();
        SinkRecord sinkRecord = pair.getLeft();
        HttpRequest httpRequest = pair.getRight();
        String bodyAsString = httpRequest.getBodyAsString();
        String pattern = getSplitPattern();
        if (HttpRequest.BodyType.STRING.equals(httpRequest.getBodyType())
                && pattern != null && bodyAsString!=null && !bodyAsString.isBlank()) {
            ArrayList<String> parts = Lists.newArrayList(bodyAsString.split(pattern, getSplitLimit()));
            httpRequests.addAll(parts.stream()
                    .map(part -> {
                        HttpRequest partRequest = new HttpRequest(httpRequest);
                        partRequest.setBodyAsString(part);
                        return Pair.of(sinkRecord, partRequest);
                    })
                    .collect(Collectors.toList()));
        } else {
            //no splitter
            httpRequests.add(Pair.of(sinkRecord, httpRequest));
        }
        return httpRequests;
    }
}
