package io.github.clescot.kafka.connect;

import java.util.Map;
import java.util.stream.Collectors;

public class MapUtils {


    private MapUtils() {
        // Utility class, no instantiation
    }

    /**
     * Returns a new map containing only the entries from the original map
     * whose keys start with the specified prefix. The prefix is removed from the keys.
     * @param map
     * @param prefix
     * @return
     */
    public static Map<String,String> getMapWithPrefix(Map<String, String> map, String prefix) {
        return map.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(prefix.length()),
                        Map.Entry::getValue
                ));
    }



    public static <T> Map<String,T> filterEntriesStartingWithPrefixes(Map<String, T> map, String... prefixes) {
        return map.entrySet().stream()
                .filter(entry -> {
                    for (String prefix : prefixes) {
                        if (entry.getKey().startsWith(prefix)) {
                            return true;
                        }
                    }
                    return false;
                })
                .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue)
                );
    }


}
