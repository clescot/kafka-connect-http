package io.github.clescot.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collection;

public class ConfigUtils {

    public static ConfigDef mergeConfigDefs(ConfigDef... configDefs) {
        ConfigDef mergedConfigDef = new ConfigDef();
        for (ConfigDef configDef : configDefs) {
            Collection<ConfigDef.ConfigKey> values = configDef.configKeys().values();
            for (ConfigDef.ConfigKey value : values) {
                mergedConfigDef = mergedConfigDef.define(value);
            }
        }
        return mergedConfigDef;
    }
}
