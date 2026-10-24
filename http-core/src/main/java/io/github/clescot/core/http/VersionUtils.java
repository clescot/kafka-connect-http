package io.github.clescot.core.http;

import java.io.IOException;
import java.util.Properties;

public class VersionUtils {

    private final static VersionUtils INSTANCE = new VersionUtils();
    public final static String VERSION = INSTANCE.getVersion();
    private VersionUtils(){}

    public String getVersion(){
        final Properties properties = new Properties();
        try {
            properties.load(VersionUtils.class.getClassLoader().getResourceAsStream("project.properties"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return (String) properties.get("version");
    }



}
