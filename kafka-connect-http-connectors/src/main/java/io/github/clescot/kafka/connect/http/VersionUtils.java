package io.github.clescot.kafka.connect.http;

import com.google.common.base.Strings;

public class VersionUtils {

        private VersionUtils() {
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
