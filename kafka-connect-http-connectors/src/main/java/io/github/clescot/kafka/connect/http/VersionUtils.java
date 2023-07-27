package io.github.clescot.kafka.connect.http;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.util.Properties;

public class VersionUtils {


    public String getVersion(){
        final Properties properties = new Properties();
        try {
            properties.load(VersionUtils.class.getClassLoader().getResourceAsStream("project.properties"));
        } catch (IOException e) {
            throw new ConnectException(e);
        }
        return (String) properties.get("version");
    }



}
