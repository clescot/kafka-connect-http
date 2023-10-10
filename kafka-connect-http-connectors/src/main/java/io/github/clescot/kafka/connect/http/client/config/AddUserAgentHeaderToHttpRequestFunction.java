package io.github.clescot.kafka.connect.http.client.config;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.UnaryOperator;

public class AddUserAgentHeaderToHttpRequestFunction implements UnaryOperator<HttpRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddUserAgentHeaderToHttpRequestFunction.class);
    public AddUserAgentHeaderToHttpRequestFunction() {
    }

    @Override
    public HttpRequest apply(HttpRequest httpRequest) {
//        if(!httpRequest.getHeaders().containsKey("User-Agent")){
//            String activateUserAgentInterceptor = (String) config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_USER_AGENT_OVERRIDE,"http_client");
//            if ("http_client".equalsIgnoreCase(activateUserAgentInterceptor)) {
//                LOGGER.trace("user agent interceptor : 'http_client' configured. No need to activate UserAgentInterceptor");
//            }else if("project".equalsIgnoreCase(activateUserAgentInterceptor)){
//                VersionUtils versionUtils = new VersionUtils();
//                String projectUserAgent = "Mozilla/5.0 (compatible;kafka-connect-http/"+ versionUtils.getVersion() +";"++"https://github.com/clescot/kafka-connect-http)";
//                httpClientBuilder.addNetworkInterceptor(new UserAgentInterceptor(Lists.newArrayList(projectUserAgent), random));
//            }else if("custom".equalsIgnoreCase(activateUserAgentInterceptor)){
//                String userAgentValuesAsString = config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_USER_AGENT_CUSTOM_VALUES, StringUtils.EMPTY).toString();
//                List<String> userAgentValues = Arrays.asList(userAgentValuesAsString.split("\\|"));
//                httpClientBuilder.addNetworkInterceptor(new UserAgentInterceptor(userAgentValues, random));
//            }else{
//                LOGGER.trace("user agent interceptor : '{}' configured. No need to activate UserAgentInterceptor",activateUserAgentInterceptor);
//            }
//        }else {
            return httpRequest;
//        }
    }
}
