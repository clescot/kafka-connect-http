package io.github.clescot.kafka.connect.http.client.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.UnaryOperator;

public class AddUserAgentHeaderToHttpRequestFunction implements UnaryOperator<HttpRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddUserAgentHeaderToHttpRequestFunction.class);
    public static final String USER_AGENT = "User-Agent";
    private final List<String> userAgents;
    private final Random random;

    public AddUserAgentHeaderToHttpRequestFunction(List<String> userAgents, Random random) {
        Preconditions.checkNotNull(userAgents);
        Preconditions.checkNotNull(random);
        Preconditions.checkArgument(!userAgents.isEmpty(),"userAgents list is empty");
        this.userAgents = userAgents;
        this.random = random;
    }

    @Override
    public HttpRequest apply(HttpRequest httpRequest) {
        if (!httpRequest.getHeaders().containsKey(USER_AGENT)) {
            Map<String, List<String>> httpRequestHeaders = httpRequest.getHeaders();
            String userAgent = getUserAgents();
            LOGGER.debug("User-Agent:{}", userAgent);
            httpRequestHeaders.put(USER_AGENT, Lists.newArrayList(userAgent));
        }
        return httpRequest;
    }

    private String getUserAgents() {
        return userAgents.get(random.nextInt(userAgents.size()));
    }
}
