package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public class MessageSplitter {

    private final String id;
    private final String splitPattern;
    private final int splitLimit;
    public static final String SINK_RECORD = "sinkRecord";
    private final JexlExpression jexlMatchingExpression;

    public MessageSplitter(String id,
                           JexlEngine jexlEngine, String matchingExpression, String splitPattern,
                           int splitLimit) {
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

    public boolean matches(SinkRecord sinkRecord) {
        // populate the context
        JexlContext context = new MapContext();
        context.set(SINK_RECORD, sinkRecord);
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

    public List<SinkRecord> split(@NotNull SinkRecord sinkRecord){
        Object value = sinkRecord.value();
        if(value!=null && value.getClass().isAssignableFrom(String.class)){
            String body = (String)value;
            List<String> list = split(body);
            return list.stream().map(content-> new SinkRecord(
                    sinkRecord.topic(),
                    sinkRecord.kafkaPartition(),
                    sinkRecord.keySchema(),
                    sinkRecord.key(),
                    sinkRecord.valueSchema(),
                    content,
                    -1,
                    sinkRecord.timestamp(),
                    sinkRecord.timestampType(),
                    sinkRecord.headers())).collect(Collectors.toList());
        }else{
            return List.of(sinkRecord);
        }
    }
}
