package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RequestGrouper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestGrouper.class);
    private final String id;
    private final Predicate<HttpRequest> predicate;
    private final String init;
    private final String separator;
    private final String end;
    private final int messageLimit;
    private final long bodyLimit;


    public RequestGrouper(String id,
                          Predicate<HttpRequest> predicate,
                          String separator,
                          String start,
                          String end,
                          int messageLimit,
                          int bodyLimit
                   ) {
        this.id = id;
        this.predicate = predicate;
        this.separator = separator;
        this.init = start;
        this.end = end;
        this.messageLimit = messageLimit;
        this.bodyLimit = bodyLimit;
    }


    private boolean matches(HttpRequest httpRequest) {
        return this.predicate.test(httpRequest);
    }

    public List<Pair<SinkRecord, HttpRequest>> group(List<Pair<SinkRecord, HttpRequest>> entries){

        if(entries==null || entries.isEmpty()){
            return Lists.newArrayList();
        }
        LOGGER.debug("'{}' grouping requests",getId());
        HttpRequest aggregatedRequest = (HttpRequest) (entries.get(0).getRight()).clone();
        String aggregatedBody=init;
        int consumed = 0;
        StringBuilder builder = new StringBuilder(aggregatedBody);
        boolean interrupted=false;
        List<Pair<SinkRecord, HttpRequest>> matchingEntries = entries.stream().filter(pair-> this.matches(pair.getRight())).collect(Collectors.toList());
        List<Pair<SinkRecord, HttpRequest>> nonMatchingEntries = entries.stream().filter(pair-> !this.matches(pair.getRight())).collect(Collectors.toList());
        for (int i = 0; i < matchingEntries.size(); i++) {
            Pair<SinkRecord, HttpRequest> myEntry = matchingEntries.get(i);
            String part = myEntry.getRight().getBodyAsString();
            if((messageLimit>0 && i==messageLimit)||(bodyLimit!=-1 && builder.length()+part.length()>=bodyLimit)){
                consumed = i;
                interrupted = true;
                break;
            }
            if(i>0) {
                builder.append(separator);
            }
            builder.append(part);
        }
        if(!interrupted){
            consumed = entries.size();
        }
        if(end!=null) {
            builder.append(end);
        }
        aggregatedBody = builder.toString();
        aggregatedRequest.setBodyAsString(aggregatedBody);
        List<Pair<SinkRecord, HttpRequest>> nonAggregatedRequests = entries.subList(consumed, entries.size());
        List<Pair<SinkRecord, HttpRequest>> aggregatedRequests = Lists.newArrayList();
        aggregatedRequests.add(Pair.of(entries.get(0).getLeft(),aggregatedRequest));
        aggregatedRequests.addAll(group(nonAggregatedRequests));
        aggregatedRequests.addAll(nonMatchingEntries);
        return aggregatedRequests;
    }

    public String getId() {
        return id;
    }

}
