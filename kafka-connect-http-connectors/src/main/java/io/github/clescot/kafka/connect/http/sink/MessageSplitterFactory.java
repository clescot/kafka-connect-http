package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.jexl3.JexlEngine;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.MESSAGE_SPLITTER_IDS;

public class MessageSplitterFactory {
    public static final String MESSAGE_SPLITTER = "message.splitter.";
    public List<MessageSplitter> buildMessageSplitters(HttpSinkConnectorConfig connectorConfig, JexlEngine jexlEngine) {
        List<MessageSplitter> requestSplitterList = Lists.newArrayList();
        for (String splitterId : Optional.ofNullable(connectorConfig.getList(MESSAGE_SPLITTER_IDS)).orElse(Lists.newArrayList())) {
            Map<String, Object> settings = connectorConfig.originalsWithPrefix(MESSAGE_SPLITTER + splitterId + ".");
            String splitPattern = (String) settings.get("pattern");
            Preconditions.checkNotNull(splitPattern,"message splitter '"+splitterId+"' splitPattern is required");
            String limit = (String) settings.get("limit");
            int splitLimit = 0;
            if(limit!=null&& !limit.isBlank()) {
                splitLimit = Integer.parseInt(limit);
            }
            String matchingExpression = (String) settings.get("matcher");
            MessageSplitter requestSplitter = new MessageSplitter(splitterId,jexlEngine,matchingExpression,splitPattern,splitLimit);
            requestSplitterList.add(requestSplitter);
        }
        return requestSplitterList;
    }
}
