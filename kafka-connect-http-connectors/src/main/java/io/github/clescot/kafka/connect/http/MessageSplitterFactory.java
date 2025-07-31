package io.github.clescot.kafka.connect.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.MapUtils;
import org.apache.commons.jexl3.JexlEngine;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MessageSplitterFactory {
    public static final String MESSAGE_SPLITTER = "message.splitter.";
    public List<MessageSplitter> buildMessageSplitters(Map<String, String> config, JexlEngine jexlEngine, List<String> messageSplitterIds) {
        List<MessageSplitter> requestSplitterList = Lists.newArrayList();
        for (String splitterId : Optional.ofNullable(messageSplitterIds).orElse(Lists.newArrayList())) {
            Map<String, String> settings = MapUtils.getMapWithPrefix(config,MESSAGE_SPLITTER + splitterId + ".");
            String splitPattern = settings.get("pattern");
            Preconditions.checkNotNull(splitPattern,"message splitter '"+splitterId+"' splitPattern is required");
            String limit = settings.get("limit");
            int splitLimit = 0;
            if(limit!=null&& !limit.isBlank()) {
                splitLimit = Integer.parseInt(limit);
            }
            String matchingExpression = settings.get("matcher");
            MessageSplitter requestSplitter = new MessageSplitter(splitterId,jexlEngine,matchingExpression,splitPattern,splitLimit);
            requestSplitterList.add(requestSplitter);
        }
        return requestSplitterList;
    }
}
