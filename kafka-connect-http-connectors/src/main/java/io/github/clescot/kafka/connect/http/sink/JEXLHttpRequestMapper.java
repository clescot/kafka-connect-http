package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class JEXLHttpRequestMapper implements HttpRequestMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(FreeMarkerHttpRequestMapper.class);
    public static final String SELECTOR_TEMPLATE_NAME = "selector";
    public static final String MATCHES = "MATCHES";
    private JexlFeatures features = new JexlFeatures()
            .loops(false)
            .sideEffectGlobal(false)
            .sideEffect(false);
    private final JexlEngine jexl;
    private JexlExpression jexlMatchingExpression;
    private JexlExpression jexlUrlExpression;
    private Optional<JexlExpression> jexlMethodExpression;
    private Optional<JexlExpression> jexlBodyTypeExpression;


    public JEXLHttpRequestMapper(@NotNull String matchingExpression,
                                 @NotNull String urlExpression,
                                 @Nullable String methodExpression,
                                 @Nullable String bodyTypeExpression) {
        Preconditions.checkNotNull(matchingExpression);
        Preconditions.checkArgument(!matchingExpression.isEmpty());
        // Restricted permissions to a safe set but with URI allowed
        JexlPermissions permissions = new JexlPermissions.ClassPermissions(SinkRecord.class, ConnectRecord.class,HttpRequest.class);
        // Create the engine
        jexl = new JexlBuilder().features(features).permissions(permissions).create();
        jexlMatchingExpression = jexl.createExpression(matchingExpression);
        jexlUrlExpression = jexl.createExpression(urlExpression);
        jexlMethodExpression = methodExpression!=null?Optional.of(jexl.createExpression(methodExpression)):Optional.empty();
        jexlBodyTypeExpression = bodyTypeExpression!=null?Optional.of(jexl.createExpression(bodyTypeExpression)):Optional.empty();
        //TODO headers
    }

    @Override
    public boolean matches(SinkRecord sinkRecord) {
        // populate the context
        JexlContext context = new MapContext();
        context.set("sinkRecord", sinkRecord);
        return (boolean) jexlMatchingExpression.evaluate(context);
    }

    @Override
    public HttpRequest map(SinkRecord sinkRecord) {
        JexlContext context = new MapContext();
        context.set("sinkRecord", sinkRecord);

        String url = (String) jexlUrlExpression.evaluate(context);
        String method = jexlMethodExpression.isPresent()?(String) jexlMethodExpression.get().evaluate(context):"GET";
        String bodyType = jexlBodyTypeExpression.isPresent()? HttpRequest.BodyType.valueOf((String)jexlBodyTypeExpression.get().evaluate(context)).name(): HttpRequest.BodyType.STRING.name();
        HttpRequest httpRequest = new HttpRequest(url,method,bodyType);

        return httpRequest;
    }
}
