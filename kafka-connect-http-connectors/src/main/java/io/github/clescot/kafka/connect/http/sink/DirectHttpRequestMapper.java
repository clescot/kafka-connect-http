package io.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Maps;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Map a sinkRecord already prepared to be parsed directly as an {@link HttpRequest}.
 */
public class DirectHttpRequestMapper implements HttpRequestMapper{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectHttpRequestMapper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    public static final String SINK_RECORD_HAS_GOT_A_NULL_VALUE = "sinkRecord has got a 'null' value";
    private final Template selectorTemplate;

    public DirectHttpRequestMapper(Configuration configuration,String sourceCode) {
        try {
            this.selectorTemplate = new Template("matches",sourceCode, configuration);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public boolean matches(SinkRecord sinkRecord) {
        StringWriter stringWriter = new StringWriter();
        boolean result;
        try {
            Map<String,Object> root = Maps.newHashMap();
            root.put("sinkRecord",sinkRecord);
            selectorTemplate.process(root, stringWriter);
            result = Boolean.parseBoolean(stringWriter.toString());
        } catch (TemplateException | IOException e) {
            throw new ConnectException(e);
        }
        return result;
    }

    @Override
    public HttpRequest map(SinkRecord sinkRecord) {
        if (sinkRecord == null || sinkRecord.value() == null) {
            LOGGER.warn(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
            throw new ConnectException(SINK_RECORD_HAS_GOT_A_NULL_VALUE);
        }
        HttpRequest httpRequest = null;
        Object value = sinkRecord.value();
        Class<?> valueClass = value.getClass();
        String stringValue = null;

        if (Struct.class.isAssignableFrom(valueClass)) {
            Struct valueAsStruct = (Struct) value;
            LOGGER.debug("Struct is {}", valueAsStruct);
            valueAsStruct.validate();
            Schema schema = valueAsStruct.schema();
            String schemaTypeName = schema.type().getName();
            LOGGER.debug("schema type name referenced in Struct is '{}'", schemaTypeName);
            Integer version = schema.version();
            LOGGER.debug("schema version referenced in Struct is '{}'", version);

            httpRequest = HttpRequestAsStruct
                    .Builder
                    .anHttpRequest()
                    .withStruct(valueAsStruct)
                    .build();
            LOGGER.debug("httpRequest : {}", httpRequest);
        } else if (byte[].class.isAssignableFrom(valueClass)) {
            //we assume the value is a byte array
            stringValue = new String((byte[]) value, StandardCharsets.UTF_8);
            LOGGER.debug("byte[] is {}", stringValue);
        } else if (String.class.isAssignableFrom(valueClass)) {
            stringValue = (String) value;
            LOGGER.debug("String is {}", stringValue);
        } else {
            LOGGER.warn("value is an instance of the class '{}' not handled by the WsSinkTask", valueClass.getName());
            throw new ConnectException("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
        }
        //valueClass is not a Struct, but a String/byte[]
        if (httpRequest == null) {
            LOGGER.debug("stringValue :{}", stringValue);
            httpRequest = parseHttpRequestAsJsonString(stringValue);
            LOGGER.debug("successful httpRequest parsing :{}", httpRequest);
        }

        return httpRequest;
    }


    private HttpRequest parseHttpRequestAsJsonString(String value) throws ConnectException {
        HttpRequest httpRequest;
        try {
            httpRequest = OBJECT_MAPPER.readValue(value, HttpRequest.class);
        } catch (JsonProcessingException e) {
            throw new ConnectException(e);
        }
        return httpRequest;
    }
}
