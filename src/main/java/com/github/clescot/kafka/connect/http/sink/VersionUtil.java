package com.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Strings;

public class VersionUtil {

    private VersionUtil() {
        // Class with static methods
    }

    public static String version(Class<?> cls) {
        String result = cls.getPackage().getImplementationVersion();

        if (Strings.isNullOrEmpty(result)) {
            result = "0.0.0";
        }

        return result;
    }
}