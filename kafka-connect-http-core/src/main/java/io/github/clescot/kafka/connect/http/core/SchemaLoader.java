package io.github.clescot.kafka.connect.http.core;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SchemaLoader {

    private static String loadSchema(String schemaPath) throws IOException, URISyntaxException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(schemaPath);
        if (url == null) {
            throw new IllegalStateException("Schema file not found: " + schemaPath);
        }
        Path path = Paths.get(url.toURI());
        return Files.readString(path);
    }

    public static ParsedSchema loadHttpRequestSchema(){
        try {
            return  new JsonSchema(loadSchema("schemas/json/versions/2/http-request.json"));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    public static ParsedSchema loadHttpResponseSchema(){
        try {
            return new JsonSchema(loadSchema("schemas/json/versions/2/http-response.json"));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static ParsedSchema loadHttpPartSchema(){
        try {
            return  new JsonSchema(loadSchema("schemas/json/versions/2/http-part.json"));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    public static ParsedSchema loadHttpExchangeSchema(){
        try {
            return  new JsonSchema(loadSchema("schemas/json/versions/2/http-exchange.json"));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
