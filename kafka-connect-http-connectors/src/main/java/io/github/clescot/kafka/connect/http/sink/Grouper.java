package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.sink.mapper.HttpRequestMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Predicate;

public class Grouper {

    private static final Logger LOGGER = LoggerFactory.getLogger(Grouper.class);
    private final String id;
    private final Predicate<HttpRequest> predicate;

    public Grouper(String id,
                   Predicate<HttpRequest> predicate
                   ) {
        this.id = id;
        this.predicate = predicate;
    }


    private List<Pair<SinkRecord, HttpRequest>> reduce(
            HttpRequestMapper httpRequestMapper,
            List<Pair<SinkRecord, HttpRequest>> entries
    ){
        String init ="#";
        String separator ="|";
        String end="@";
        int messageLimit=10;
        long bodyLimit=1000;
        if(entries.isEmpty()){
            return Lists.newArrayList();
        }
        LOGGER.debug("consuming requests from '{}'",httpRequestMapper.getId());
        HttpRequest aggregatedRequest = new HttpRequest(entries.get(0).getRight());
        String aggregatedBody=init;
        int consumed = 0;
        StringBuilder builder = new StringBuilder(aggregatedBody);
        boolean interrupted=false;
        for (int i = 0; i < entries.size(); i++) {
            Pair<SinkRecord, HttpRequest> myEntry = entries.get(i);
            String part = myEntry.getRight().getBodyAsString();
            if(i==messageLimit||builder.length()+part.length()>=bodyLimit){
                consumed = i;
                interrupted = true;
                break;
            }
            builder.append(part);
            builder.append(separator);
            i++;
        }
        if(!interrupted){
            consumed = entries.size();
        }
        builder.append(end);
        aggregatedBody = builder.toString();
        aggregatedRequest.setBodyAsString(aggregatedBody);
        List<Pair<SinkRecord, HttpRequest>> nonAgregatedRequests = entries.subList(consumed, entries.size());
        List<Pair<SinkRecord, HttpRequest>> agregatedRequests = Lists.newArrayList();
        agregatedRequests.add(Pair.of(entries.get(0).getLeft(),aggregatedRequest));
        agregatedRequests.addAll(reduce(httpRequestMapper, nonAgregatedRequests));
        return agregatedRequests;
    }

//
//    Map<String, List<Triple<SinkRecord, HttpRequest, HttpRequestMapper>>> collected = stream
//            .peek(this::debugConnectRecord)
//            .filter(sinkRecord -> sinkRecord.value() != null)
//            .map(this::toHttpRequests)
//            .flatMap(List::stream)
//            .collect(Collectors.groupingBy(triple->triple.getRight().getId()+"-"+triple.getMiddle().getUrl()));
//
//    List<HttpExchange> httpExchangeList = collected.entrySet().stream().map(
//                    entry -> {
//
//                        LOGGER.debug("consuming requests from '{}'", entry.getKey());
//                        List<Triple<SinkRecord, HttpRequest, HttpRequestMapper>> list = entry.getValue();
//                        List<Pair<SinkRecord, HttpRequest>> entries = list
//                                .stream()
//                                .map(triple -> Pair.of(triple.getLeft(), triple.getMiddle()))
//                                .collect(Collectors.toList());
//                        //list has got at least one entry
//                        HttpRequestMapper httpRequestMapper = list.get(0).getRight();
//                        List<Pair<SinkRecord, HttpRequest>> aggregatedRequests = Lists.newArrayList();
////                            List<Pair<SinkRecord, HttpRequest>> aggregatedRequests = reduce(httpRequestMapper, entries);
//                        List<HttpExchange> httpExchanges = aggregatedRequests.stream()
//                                .map(this::call)
//                                .map(CompletableFuture::join).collect(Collectors.toList());
//                        LOGGER.debug("{} - HttpExchanges created :'{}'", httpRequestMapper.getId(), httpExchanges.size());
//                        return Map.entry(entry.getKey(), httpExchanges);
//                    }
//            ).flatMap(entry -> entry.getValue().stream())
//            .collect(Collectors.toList());
//        LOGGER.debug("httpExchange processed :{}",httpExchangeList.size());


}
