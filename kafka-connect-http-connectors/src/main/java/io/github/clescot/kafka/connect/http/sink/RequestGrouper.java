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
    private final long bodyLimit = -1;


    public RequestGrouper(String id,
                          Predicate<HttpRequest> predicate,
                          String separator,
                          String start,
                          String end,
                          int messageLimit
                   ) {
        this.id = id;
        this.predicate = predicate;
        this.separator = separator;
        this.init = start;
        this.end = end;
        this.messageLimit = messageLimit;
    }


    private boolean matches(HttpRequest httpRequest) {
        return this.predicate.test(httpRequest);
    }

    public List<Pair<SinkRecord, HttpRequest>> group(List<Pair<SinkRecord, HttpRequest>> entries){

        if(entries==null || entries.isEmpty()){
            return Lists.newArrayList();
        }
        LOGGER.debug("'{}' grouping requests",getId());
        HttpRequest aggregatedRequest = new HttpRequest(entries.get(0).getRight());
        String aggregatedBody=init;
        int consumed = 0;
        StringBuilder builder = new StringBuilder(aggregatedBody);
        boolean interrupted=false;
        List<Pair<SinkRecord, HttpRequest>> matchingEntries = entries.stream().filter(pair-> this.matches(pair.getRight())).collect(Collectors.toList());
        for (int i = 0; i < matchingEntries.size(); i++) {
            Pair<SinkRecord, HttpRequest> myEntry = matchingEntries.get(i);
            String part = myEntry.getRight().getBodyAsString();
            if(messageLimit>0 && (i==messageLimit||builder.length()+part.length()>=bodyLimit)){
                consumed = i;
                interrupted = true;
                break;
            }
            builder.append(part);
            builder.append(separator);
        }
        if(!interrupted){
            consumed = entries.size();
        }
        if(end!=null) {
            builder.append(end);
        }
        aggregatedBody = builder.toString();
        aggregatedRequest.setBodyAsString(aggregatedBody);
        List<Pair<SinkRecord, HttpRequest>> nonAgregatedRequests = entries.subList(consumed, entries.size());
        List<Pair<SinkRecord, HttpRequest>> agregatedRequests = Lists.newArrayList();
        agregatedRequests.add(Pair.of(entries.get(0).getLeft(),aggregatedRequest));
        agregatedRequests.addAll(nonAgregatedRequests);
        return agregatedRequests;
    }

    public String getId() {
        return id;
    }

    public String getEnd() {
        return end;
    }


    public String getSeparator() {
        return separator;
    }

    public long getBodyLimit() {
        return bodyLimit;
    }


    public int getMessageLimit() {
        return messageLimit;
    }


    public String getInit() {
        return init;
    }


}
