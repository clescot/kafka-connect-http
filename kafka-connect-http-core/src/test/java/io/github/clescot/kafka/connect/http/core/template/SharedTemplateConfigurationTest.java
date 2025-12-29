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
        ExchangeTemplateManager manager1 = TemplateConfigurationUtil.createTemplateManager(settings1);
        ExchangeTemplateManager manager2 = TemplateConfigurationUtil.createTemplateManager(settings2);
        
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
        ExchangeTemplateManager manager = TemplateConfigurationUtil.createTemplateManager(settings);
        
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
        ExchangeTemplateManager manager = TemplateConfigurationUtil.createTemplateManager(settings);
        
        // Verify that valid processors are registered and invalid ones are ignored
        assertThat(manager.getProcessor("jsonpath")).isNotNull();
        assertThat(manager.getProcessor("random")).isNotNull();
        assertThat(manager.getProcessor("invalid-processor")).isNull();
    }

    @Test
    void testTemplateConfigurationUtilGetSetTemplate() {
        Map<String, String> settings = new HashMap<>();
        
        // Test getting template with default
        String template = TemplateConfigurationUtil.getExchangeTemplate(settings);
        assertThat(template).isEqualTo("");
        
        // Test setting and getting template
        TemplateConfigurationUtil.setExchangeTemplate(settings, "test-template");
        template = TemplateConfigurationUtil.getExchangeTemplate(settings);
        assertThat(template).isEqualTo("test-template");
    }

    @Test
    void testTemplateConfigurationUtilRegistersAllDefaultProcessors() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
        
        // Verify that no processors are registered initially
        assertThat(manager.getProcessors()).isEmpty();
        
        // Register default processors using the shared utility
        TemplateConfigurationUtil.registerDefaultProcessors(manager);
        
        // Verify that all default processors are registered
        assertThat(manager.getProcessors()).hasSizeGreaterThanOrEqualTo(10);
        assertThat(manager.getProcessor("jsonpath")).isNotNull();
        assertThat(manager.getProcessor("xpath")).isNotNull();
        assertThat(manager.getProcessor("random")).isNotNull();
        assertThat(manager.getProcessor("jmespath")).isNotNull();
        assertThat(manager.getProcessor("regex")).isNotNull();
        assertThat(manager.getProcessor("headerparam")).isNotNull();
        assertThat(manager.getProcessor("datetime")).isNotNull();
        assertThat(manager.getProcessor("conditional")).isNotNull();
        assertThat(manager.getProcessor("hash")).isNotNull();
        assertThat(manager.getProcessor("math")).isNotNull();
    }
}