package io.github.clescot.kafka.connect.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.BiFunction;

/**
 * MessageSplitter is used to split a ConnectRecord based on a pattern.
 * It uses JEXL expressions to determine if a SinkRecord matches the splitter's criteria.
 * If it matches, it splits the message body according to the specified pattern and limit.
 */
public class MessageSplitter<T extends ConnectRecord<T>> {

    private final String id;
    private final String splitPattern;
    private final int splitLimit;
    public static final String SINK_RECORD = "sinkRecord";
    private final JexlExpression jexlMatchingExpression;
    private final BiFunction<T,String,T> fromStringPartToRecordFunction;

    public MessageSplitter(
                           BiFunction<T,String,T> fromStringPartToRecordFunction, String id,
                           JexlEngine jexlEngine,
                           String matchingExpression,
                           String splitPattern,
                           int splitLimit) {
        this.fromStringPartToRecordFunction = fromStringPartToRecordFunction;
        Preconditions.checkNotNull(id,"id is required");
        this.id = id;

        Preconditions.checkNotNull(matchingExpression,"matcher is required");
        jexlMatchingExpression = jexlEngine.createExpression(matchingExpression);
        Preconditions.checkNotNull(splitPattern,"splitPattern is required");
        this.splitPattern = splitPattern;
        this.splitLimit = splitLimit;
    }

    public String getId() {
        return id;
    }

    public int getSplitLimit() {
        return splitLimit;
    }

    public String getSplitPattern() {
        return splitPattern;
    }

    public boolean matches(T connectRecord) {
        // populate the context
        JexlContext context = new MapContext();
        context.set(SINK_RECORD, connectRecord);
        return (boolean) jexlMatchingExpression.evaluate(context);
    }

    private List<String> split(String body){
        String pattern = getSplitPattern();
        List<String> parts = Lists.newArrayList();
        if (pattern != null && body!=null && !body.isBlank()) {
            parts = Lists.newArrayList(body.split(pattern, getSplitLimit()));
        } else {
            //no splitter
            parts.add(body);
        }
        return parts;
    }

    public List<T> split(@NotNull T connectRecord){
        Object value = connectRecord.value();
        if(value!=null && value.getClass().isAssignableFrom(String.class)){
            String body = (String)value;
            List<String> list = split(body);
            return list.stream()
                    .map(part-> fromStringPartToRecordFunction.apply(connectRecord,part))
                    .toList();
        }else{
            return List.of(connectRecord);
        }
    }
}
