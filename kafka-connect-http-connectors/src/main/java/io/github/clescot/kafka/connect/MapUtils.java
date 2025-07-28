package io.github.clescot.kafka.connect;

import java.util.Map;
import java.util.stream.Collectors;

public class MapUtils {

    public static Map<String,Object> getMapWithPrefix(Map<String, Object> map, String prefix) {
        return map.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(prefix.length()),
                        Map.Entry::getValue
                ));
    }
}
