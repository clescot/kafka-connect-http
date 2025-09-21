package io.github.clescot.kafka.connect.http;

import io.github.clescot.kafka.connect.http.client.HttpException;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import java.io.IOException;
import java.util.Map;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

public class MeterRegistryFactory {


    public CompositeMeterRegistry buildMeterRegistry(Map<String, String> config) {
        CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
        boolean activateJMX = Boolean.parseBoolean(config.get(METER_REGISTRY_EXPORTER_JMX_ACTIVATE));
        if (activateJMX) {
            JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
            jmxMeterRegistry.start();
            compositeMeterRegistry.add(jmxMeterRegistry);
        }
        boolean activatePrometheus = Boolean.parseBoolean(config.get(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE));
        if (activatePrometheus) {
            PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Integer prometheusPort = Integer.parseInt(config.get(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT));
            // you can set the daemon flag to false if you want the server to block

            try {
                int port = prometheusPort!=null?prometheusPort:9090;
                PrometheusRegistry prometheusRegistry = prometheusMeterRegistry.getPrometheusRegistry();
                HTTPServer.builder()
                        .port(port)
                        .registry(prometheusRegistry)
                        .buildAndStart();
            } catch (IOException e) {
                throw new HttpException(e);
            }
            compositeMeterRegistry.add(prometheusMeterRegistry);
        }
        if(!activateJMX && !activatePrometheus){
            compositeMeterRegistry.add(new SimpleMeterRegistry());
        }
        return compositeMeterRegistry;
    }
}
