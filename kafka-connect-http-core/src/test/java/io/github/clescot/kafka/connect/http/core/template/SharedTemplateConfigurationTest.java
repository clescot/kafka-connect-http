package io.github.clescot.kafka.connect.http.core.template;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SharedTemplateConfigurationTest {

    @Test
    void testTemplateConfigurationUtilCreatesConsistentManagers() {
        // Create settings for template configuration
        Map<String, String> settings1 = new HashMap<>();
        settings1.put("exchange.template.processors", "jsonpath,random");
        
        Map<String, String> settings2 = new HashMap<>();
        settings2.put("exchange.template.processors", "jsonpath,random");
        
        // Create template managers using the shared utility
        ExchangeTemplateProcessorFactory factory = new ExchangeTemplateProcessorFactory();
        ExchangeTemplateManager manager1 = factory.createTemplateManager(settings1);
        ExchangeTemplateManager manager2 = factory.createTemplateManager(settings2);
        
        // Verify that both managers have the same processors
        assertThat(manager1.getProcessor("jsonpath")).isNotNull();
        assertThat(manager2.getProcessor("jsonpath")).isNotNull();
        assertThat(manager1.getProcessor("random")).isNotNull();
        assertThat(manager2.getProcessor("random")).isNotNull();
    }

    @Test
    void testTemplateConfigurationUtilHandlesCustomProcessors() {
        // Create settings with custom processors
        Map<String, String> settings = new HashMap<>();
        settings.put("exchange.template.processors", "jsonpath,datetime,hash");
        
        // Create template manager using the shared utility
        ExchangeTemplateProcessorFactory factory = new ExchangeTemplateProcessorFactory();
        ExchangeTemplateManager manager = factory.createTemplateManager(settings);
        
        // Verify that all requested processors are registered
        assertThat(manager.getProcessor("jsonpath")).isNotNull();
        assertThat(manager.getProcessor("datetime")).isNotNull();
        assertThat(manager.getProcessor("hash")).isNotNull();
    }

    @Test
    void testTemplateConfigurationUtilHandlesInvalidProcessors() {
        // Create settings with invalid processor names
        Map<String, String> settings = new HashMap<>();
        settings.put("exchange.template.processors", "jsonpath,invalid-processor,random");
        
        // Create template manager using the shared utility
        ExchangeTemplateProcessorFactory factory = new ExchangeTemplateProcessorFactory();
        ExchangeTemplateManager manager = factory.createTemplateManager(settings);
        
        // Verify that valid processors are registered and invalid ones are ignored
        assertThat(manager.getProcessor("jsonpath")).isNotNull();
        assertThat(manager.getProcessor("random")).isNotNull();
        assertThat(manager.getProcessor("invalid-processor")).isNull();
    }

}