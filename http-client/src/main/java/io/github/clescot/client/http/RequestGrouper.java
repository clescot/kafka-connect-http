package io.github.clescot.client.http;

import com.google.common.collect.Lists;
import io.github.clescot.core.http.HttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Predicate;

/**
 * RequestGrouper is used to group HttpRequests based on a predicate.
 * It aggregates the body of matching requests and returns a list of aggregated requests.
 * Non-matching requests are returned as they are.
 */
public class RequestGrouper<T> {

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

    /**
     *
     * @param entries if records are aggregated, the return aggregated pair from the list is linked with the record from the first request
     * aggregated.
     * @return
     */
    public List<Pair<T, HttpRequest>> group(List<Pair<T, HttpRequest>> entries){

        if(entries==null || entries.isEmpty()){
            return Lists.newArrayList();
        }
        LOGGER.debug("'{}' grouping requests",getId());

        List<Pair<T, HttpRequest>> matchingEntries = entries.stream()
                .filter(pair-> this.matches(pair.getRight()))
                .toList();

        List<Pair<T, HttpRequest>> nonMatchingEntries = entries.stream()
                .filter(pair-> !this.matches(pair.getRight()))
                .toList();

        List<Pair<T, HttpRequest>> aggregatedRequests = Lists.newArrayList();
        int index = 0;
        while (index < matchingEntries.size()) {
            //the aggregate is cloned from the first matching entry, so it keeps its url, headers and method
            Pair<T, HttpRequest> firstMatchingEntry = matchingEntries.get(index);
            HttpRequest aggregatedRequest = (HttpRequest) firstMatchingEntry.getRight().clone();
            StringBuilder builder = new StringBuilder(init);
            int grouped = 0;
            while (index < matchingEntries.size()) {
                String part = bodyOf(matchingEntries.get(index).getRight());
                boolean messageLimitReached = messageLimit > 0 && grouped >= messageLimit;
                boolean bodyLimitReached = bodyLimit != -1 && builder.length() + part.length() >= bodyLimit;
                //a group always consumes its first matching entry, otherwise it would never terminate
                if (grouped > 0 && (messageLimitReached || bodyLimitReached)) {
                    break;
                }
                if (grouped > 0) {
                    builder.append(separator);
                }
                builder.append(part);
                grouped++;
                index++;
            }
            if(end!=null) {
                builder.append(end);
            }
            aggregatedRequest.setBodyAsString(builder.toString());
            aggregatedRequests.add(Pair.of(firstMatchingEntry.getLeft(), aggregatedRequest));
        }
        aggregatedRequests.addAll(nonMatchingEntries);
        return aggregatedRequests;
    }

    private String bodyOf(HttpRequest httpRequest) {
        String body = httpRequest.getBodyAsString();
        return body != null ? body : "";
    }

    public String getId() {
        return id;
    }

}
