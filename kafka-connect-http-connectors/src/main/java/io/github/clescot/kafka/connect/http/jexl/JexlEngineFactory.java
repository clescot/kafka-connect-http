package io.github.clescot.kafka.connect.http.jexl;

import io.github.clescot.core.http.HttpRequest;
import java.util.Map;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Builds the sandboxed JEXL engine used by the sink-side expressions. The engine is restricted to a
 * safe set of classes and exposes the {@code datetime} namespace.
 */
public final class JexlEngineFactory {

  private JexlEngineFactory() {}

  public static JexlEngine build() {
    // Restricted permissions to a safe set but with URI allowed
    JexlPermissions permissions =
        new JexlPermissions.ClassPermissions(
            SinkRecord.class, ConnectRecord.class, HttpRequest.class, DateTimeFunctions.class);
    // Create the engine
    JexlFeatures features =
        new JexlFeatures().loops(false).sideEffectGlobal(false).sideEffect(false);
    return new JexlBuilder()
        .features(features)
        .permissions(permissions)
        .namespaces(Map.of(DateTimeFunctions.NAMESPACE, DateTimeFunctions.class))
        .create();
  }
}
