package io.github.clescot.kafka.connect.http.sink.mapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.sink.HttpSinkConnectorConfig;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.REQUEST_MAPPER_DEFAULT_URL_EXPRESSION;

public class HttpRequestMapperFactory {
    public static final String DEFAULT = "default";

    public static final String JEXL_ALWAYS_MATCHES = "true";

    public HttpRequestMapper buildDefaultHttpRequestMapper(HttpSinkConnectorConfig connectorConfig, JexlEngine jexlEngine){
        HttpRequestMapper httpRequestMapper;
        MapperMode defaultRequestMapperMode = connectorConfig.getDefaultRequestMapperMode();
        switch (defaultRequestMapperMode) {
            case JEXL: {
                Preconditions.checkNotNull(connectorConfig.getDefaultUrlExpression(), "'" + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION + "' need to be set");
                httpRequestMapper = new JEXLHttpRequestMapper(
                        DEFAULT,
                        jexlEngine,
                        JEXL_ALWAYS_MATCHES,
                        connectorConfig.getDefaultUrlExpression(),
                        connectorConfig.getDefaultMethodExpression(),
                        connectorConfig.getDefaultBodyTypeExpression(),
                        connectorConfig.getDefaultBodyExpression(),
                        connectorConfig.getDefaultHeadersExpression()
                );
                break;
            }
            case DIRECT:
            default: {
                httpRequestMapper = new DirectHttpRequestMapper(
                        DEFAULT,
                        jexlEngine,
                        JEXL_ALWAYS_MATCHES
                );
                break;
            }
        }

        return httpRequestMapper;
    }

    public List<HttpRequestMapper> buildCustomHttpRequestMappers(AbstractConfig config, JexlEngine jexlEngine, List<String> requestMapperIds) {
        List<HttpRequestMapper> requestMappers = Lists.newArrayList();
        for (String httpRequestMapperId : Optional.ofNullable(requestMapperIds).orElse(Lists.newArrayList())) {
            HttpRequestMapper httpRequestMapper;
            String prefix = "http.request.mapper." + httpRequestMapperId;
            Map<String, Object> settings = config.originalsWithPrefix(prefix);
            String modeKey = ".mode";
            MapperMode mapperMode = MapperMode.valueOf(Optional.ofNullable(settings.get(modeKey)).orElse(MapperMode.DIRECT.name()).toString());
            switch (mapperMode) {
                case JEXL: {
                    httpRequestMapper = new JEXLHttpRequestMapper(
                            httpRequestMapperId,
                            jexlEngine,
                            (String) settings.get(".matcher"),
                            (String) settings.get(".url"),
                            (String) settings.get(".method"),
                            (String) settings.get(".bodytype"),
                            (String) settings.get(".body"),
                            (String) settings.get(".headers")
                    );
                    break;
                }
                case DIRECT:
                default: {
                    httpRequestMapper = new DirectHttpRequestMapper(
                            httpRequestMapperId,
                            jexlEngine,
                            (String) settings.get(".matcher")
                    );
                    break;
                }
            }

            requestMappers.add(httpRequestMapper);
        }
        return requestMappers;
    }

}
