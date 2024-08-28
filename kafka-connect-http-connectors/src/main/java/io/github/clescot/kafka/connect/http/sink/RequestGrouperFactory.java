package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder;
import io.github.clescot.kafka.connect.http.core.HttpRequest;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.REQUEST_GROUPER_IDS;

public class RequestGrouperFactory {

    public static final String REQUEST_GROUPER = "request.grouper.";

    public List<RequestGrouper> buildRequestGroupers(HttpSinkConnectorConfig connectorConfig) {
        List<RequestGrouper> requestGrouperList = Lists.newArrayList();
        for (String requestGrouperId : Optional.ofNullable(connectorConfig.getList(REQUEST_GROUPER_IDS)).orElse(Lists.newArrayList())) {
            Map<String, Object> settings = connectorConfig.originalsWithPrefix(REQUEST_GROUPER + requestGrouperId + ".");
            Predicate<HttpRequest> httpRequestPredicate = HttpRequestPredicateBuilder.build().buildPredicate(settings);
            String separator = (String) settings.get("separator");
            String start = (String) settings.get("start");
            String end = (String) settings.get("end");
            int messageLimit = (int) settings.get("message.limit");
            RequestGrouper requestGrouper = new RequestGrouper(requestGrouperId,httpRequestPredicate,separator,start,end,messageLimit);
            requestGrouperList.add(requestGrouper);
        }
        return requestGrouperList;
    }
}
