package io.github.clescot.kafka.connect.http.sink.mapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.jexl3.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class JEXLHttpRequestMapper extends AbstractHttpRequestMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(JEXLHttpRequestMapper.class);
    public static final String SINK_RECORD = "sinkRecord";

    private final JexlExpression jexlMatchingExpression;
    private final JexlExpression jexlUrlExpression;
    private final Optional<JexlExpression> jexlMethodExpression;
    private final Optional<JexlExpression> jexlBodyTypeExpression;
    private final Optional<JexlExpression> jexlBodyExpression;
    private final Optional<JexlExpression> jexlHeadersExpression;


    public JEXLHttpRequestMapper(String id,
                                 JexlEngine jexlEngine,
                                 @NotNull String matchingExpression,
                                 @NotNull String urlExpression,
                                 @Nullable String methodExpression,
                                 @Nullable String bodyTypeExpression,
                                 @Nullable String bodyExpression,
                                 @Nullable String headersExpression
                                 ) {
        super(id);
        Preconditions.checkNotNull(matchingExpression);
        Preconditions.checkArgument(!matchingExpression.isEmpty());
        jexlMatchingExpression = jexlEngine.createExpression(matchingExpression);
        jexlUrlExpression = jexlEngine.createExpression(urlExpression);
        jexlMethodExpression = methodExpression!=null?Optional.of(jexlEngine.createExpression(methodExpression)):Optional.empty();
        jexlBodyTypeExpression = bodyTypeExpression!=null?Optional.of(jexlEngine.createExpression(bodyTypeExpression)):Optional.empty();
        jexlBodyExpression = bodyExpression!=null?Optional.of(jexlEngine.createExpression(bodyExpression)):Optional.empty();
        jexlHeadersExpression = headersExpression!=null?Optional.of(jexlEngine.createExpression(headersExpression)):Optional.empty();
    }

    @Override
    public boolean matches(SinkRecord sinkRecord) {
        // populate the context
        JexlContext context = new MapContext();
        context.set(SINK_RECORD, sinkRecord);
        return (boolean) jexlMatchingExpression.evaluate(context);
    }

    @Override
    public HttpRequest map(SinkRecord sinkRecord) {
        JexlContext context = new MapContext();
        context.set(SINK_RECORD, sinkRecord);
        String url = (String) jexlUrlExpression.evaluate(context);
        HttpRequest.Method method = jexlMethodExpression.map(jexlExpression -> HttpRequest.Method.valueOf((String) jexlExpression.evaluate(context))).orElse(HttpRequest.Method.GET);
        String bodyTypeAsString = jexlBodyTypeExpression.map(jexlExpression -> (String) jexlExpression.evaluate(context)).orElseGet(HttpRequest.BodyType.STRING::name);
        HttpRequest.BodyType bodyType = HttpRequest.BodyType.valueOf(bodyTypeAsString);
        String content = jexlBodyExpression.isPresent()?jexlBodyExpression.map(jexlExpression -> (String) jexlExpression.evaluate(context)).orElse(null):null;
        HttpRequest httpRequest = new HttpRequest(url,method,bodyType.name());
        switch (bodyType){
            case STRING:
            default:{
                httpRequest.setBodyAsString(content);
            }
        }
        Map<String, List<String>> headers = jexlHeadersExpression.isPresent()? (Map<String, List<String>>)jexlHeadersExpression.get().evaluate(context): Maps.newHashMap();
        httpRequest.setHeaders(headers);
        return httpRequest;
    }

    @Nullable
    public Optional<JexlExpression> getJexlBodyExpression() {
        return jexlBodyExpression;
    }

    public Optional<JexlExpression> getJexlBodyTypeExpression() {
        return jexlBodyTypeExpression;
    }

    public Optional<JexlExpression> getJexlHeadersExpression() {
        return jexlHeadersExpression;
    }

    public JexlExpression getJexlMatchingExpression() {
        return jexlMatchingExpression;
    }

    public Optional<JexlExpression> getJexlMethodExpression() {
        return jexlMethodExpression;
    }

    public JexlExpression getJexlUrlExpression() {
        return jexlUrlExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JEXLHttpRequestMapper)) return false;
        JEXLHttpRequestMapper that = (JEXLHttpRequestMapper) o;
        return Objects.equals(jexlMatchingExpression, that.jexlMatchingExpression) && Objects.equals(jexlUrlExpression, that.jexlUrlExpression) && Objects.equals(jexlMethodExpression, that.jexlMethodExpression) && Objects.equals(jexlBodyTypeExpression, that.jexlBodyTypeExpression) && Objects.equals(jexlBodyExpression, that.jexlBodyExpression) && Objects.equals(jexlHeadersExpression, that.jexlHeadersExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jexlMatchingExpression, jexlUrlExpression, jexlMethodExpression, jexlBodyTypeExpression, jexlBodyExpression, jexlHeadersExpression);
    }

    @Override
    public String toString() {
        return "JEXLHttpRequestMapper{" +
                "jexlBodyExpression=" + jexlBodyExpression +
                ", jexlBodyTypeExpression=" + jexlBodyTypeExpression +
                ", jexlHeadersExpression=" + jexlHeadersExpression +
                ", jexlMatchingExpression=" + jexlMatchingExpression +
                ", jexlMethodExpression=" + jexlMethodExpression +
                ", jexlUrlExpression=" + jexlUrlExpression +
                ", id='" + id + '\'' +
                ", splitLimit=" + splitLimit +
                ", splitPattern=" + splitPattern +
                '}';
    }
}
