package io.github.clescot.client.http;

import com.google.common.collect.Lists;
import io.github.clescot.client.MapUtils;
import io.github.clescot.client.http.config.HttpRequestPredicateBuilder;
import io.github.clescot.core.http.HttpRequest;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class RequestGrouperFactory {

    public static final String REQUEST_GROUPER = "request.grouper.";

    public <T> List<RequestGrouper<T>> buildRequestGroupers(List<String> requestGrouperIds, Map<String, String> originalsStrings) {
        List<RequestGrouper<T>> requestGrouperList = Lists.newArrayList();
        for (String requestGrouperId : Optional.ofNullable(requestGrouperIds).orElse(Lists.newArrayList())) {
            Map<String, String> settings = MapUtils.getMapWithPrefix(originalsStrings,REQUEST_GROUPER + requestGrouperId + ".");
            Predicate<HttpRequest> httpRequestPredicate = HttpRequestPredicateBuilder.build().buildPredicate(settings);
            Optional<String> separator = Optional.ofNullable(settings.get("separator"));
            Optional<String> start = Optional.ofNullable(settings.get("start"));
            Optional<String> end = Optional.ofNullable(settings.get("end"));
            Optional<String> messageLimit = Optional.ofNullable(settings.get("message.limit"));
            int messageLimitAsInt = messageLimit.map(Integer::parseInt).orElse(-1);
            Optional<String> bodyLimit = Optional.ofNullable(settings.get("body.limit"));
            int bodyLimitAsInt = bodyLimit.map(Integer::parseInt).orElse(-1);
            RequestGrouper<T> requestGrouper = new RequestGrouper<>(
                    requestGrouperId,
                    httpRequestPredicate,
                    separator.orElse(""),
                    start.orElse(""),
                    end.orElse(""),
                    messageLimitAsInt,
                    bodyLimitAsInt
            );
            requestGrouperList.add(requestGrouper);
        }
        return requestGrouperList;
    }
}
