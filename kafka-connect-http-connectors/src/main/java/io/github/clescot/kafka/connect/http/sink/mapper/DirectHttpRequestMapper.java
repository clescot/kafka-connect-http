package io.github.clescot.kafka.connect.http.sink.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Map a sinkRecord already prepared to be parsed directly as an {@link HttpRequest}.
 */
public class DirectHttpRequestMapper extends AbstractHttpRequestMapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectHttpRequestMapper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    public static final String SINK_RECORD_HAS_GOT_A_NULL_VALUE = "sinkRecord has got a 'null' value";
    private JexlExpression expression;

    public DirectHttpRequestMapper(String id, JexlEngine jexlEngine, String matchingExpression) {
        super(id);
        expression = jexlEngine.createExpression(matchingExpression);
    }

    @Override
    public boolean matches(SinkRecord sinkRecord) {
        // populate the context
        JexlContext context = new MapContext();
        context.set("sinkRecord", sinkRecord);
        return (boolean) expression.evaluate(context);
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

    public JexlExpression getExpression() {
        return expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DirectHttpRequestMapper)) return false;
        DirectHttpRequestMapper that = (DirectHttpRequestMapper) o;
        return Objects.equals(expression, that.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(expression);
    }

    @Override
    public String toString() {
        return "DirectHttpRequestMapper{" +
                "expression=" + expression +
                ", id='" + id + '\'' +
                ", splitLimit=" + splitLimit +
                ", splitPattern=" + splitPattern +
                '}';
    }
}
