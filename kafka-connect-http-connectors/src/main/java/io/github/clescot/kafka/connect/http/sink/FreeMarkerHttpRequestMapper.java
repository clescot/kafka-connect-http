package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Maps;
import freemarker.cache.StringTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class FreeMarkerHttpRequestMapper implements HttpRequestMapper {

    public static final String SELECTOR_TEMPLATE_NAME = "selector";
    private Configuration configuration;
    public FreeMarkerHttpRequestMapper(Configuration configuration,String selectorTemplateContent,Map<String, String> templates) {

       this.configuration = configuration;
        StringTemplateLoader templateLoader = new StringTemplateLoader();
        templateLoader.putTemplate(SELECTOR_TEMPLATE_NAME,selectorTemplateContent);
        for (Map.Entry<String, String> entry : templates.entrySet()) {
            templateLoader.putTemplate(entry.getKey(), entry.getValue());
        }
        configuration.setTemplateLoader(templateLoader);
    }

    @Override
    public boolean matches(SinkRecord sinkRecord) {
        return getResolvedTemplate(sinkRecord, SELECTOR_TEMPLATE_NAME).map(Boolean::parseBoolean).orElse(false);
    }

    @Override
    public HttpRequest map(SinkRecord sinkRecord) {

        String url = getResolvedTemplate(sinkRecord, "url").orElseThrow(()->new IllegalArgumentException("url cannot ne resolved"));
        Optional<String> optionalHeaders = getResolvedTemplate(sinkRecord, "url");
        Map<String, List<String>> headers = optionalHeaders.isPresent()?
                parseHeaders(optionalHeaders.get()):
                Maps.newHashMap();
        String method = getResolvedTemplate(sinkRecord, "method").orElse("GET");
        Optional<String> optionalBodyType = getResolvedTemplate(sinkRecord, "bodyType");
        HttpRequest.BodyType bodyType;
        bodyType = optionalBodyType.map(HttpRequest.BodyType::valueOf).orElse(HttpRequest.BodyType.STRING);
        Optional<String> bodyAsString = getResolvedTemplate(sinkRecord, "body");
        HttpRequest httpRequest = new HttpRequest(url,method, bodyType.name());
        httpRequest.setHeaders(headers);
        bodyAsString.ifPresent(httpRequest::setBodyAsString);
        return httpRequest;
    }

    private Map<String,List<String>> parseHeaders(String token){
        Arrays.asList(token.split(";"))
                .stream()
                .map(part->Arrays.asList(part.split(","))).collect(Collectors.toList());
        return null;
    }

    private Optional<String> getResolvedTemplate(SinkRecord sinkRecord, String templateName) {
        try {
            Optional<Template> template = Optional.ofNullable(configuration.getTemplate(templateName));
            if(template.isEmpty()){
                return Optional.empty();
            }
            StringWriter stringWriter = new StringWriter();
            Map<String,Object> root = Maps.newHashMap();
            root.put("sinkRecord",sinkRecord);
            template.get().process(root, stringWriter);
            return Optional.of(stringWriter.toString());
        } catch (TemplateException | IOException e) {
            throw new ConnectException(e);
        }
    }
}
