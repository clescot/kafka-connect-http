package io.github.clescot.kafka.connect.http.core.template;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TemplateConfigurationUtilTest {

    @Test
    void testCreateTemplateManager() {
        Map<String, String> settings = new HashMap<>();
        settings.put("exchange.template.processors", "jsonpath,random");
        
        ExchangeTemplateManager manager = TemplateConfigurationUtil.createTemplateManager(settings);
        
        assertThat(manager).isNotNull();
        assertThat(manager.getProcessors()).hasSizeGreaterThanOrEqualTo(2); // At least jsonpath and random
        assertThat(manager.getProcessor("jsonpath")).isNotNull();
        assertThat(manager.getProcessor("random")).isNotNull();
    }

    @Test
    void testGetExchangeTemplate() {
        Map<String, String> settings = new HashMap<>();
        settings.put("exchange.template", "test-template");
        
        String template = TemplateConfigurationUtil.getExchangeTemplate(settings);
        assertThat(template).isEqualTo("test-template");
    }

    @Test
    void testGetExchangeTemplateDefault() {
        Map<String, String> settings = new HashMap<>();
        
        String template = TemplateConfigurationUtil.getExchangeTemplate(settings);
        assertThat(template).isEqualTo("");
    }

    @Test
    void testSetExchangeTemplate() {
        Map<String, String> settings = new HashMap<>();
        
        TemplateConfigurationUtil.setExchangeTemplate(settings, "new-template");
        assertThat(settings.get("exchange.template")).isEqualTo("new-template");
    }

}