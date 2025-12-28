package io.github.clescot.kafka.connect.http.core.template;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class NewTemplateProcessorsTest {
    
    @Test
    void testAllNewProcessorsRegistered() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager();
        
        // Verify all new processors are registered
        assertThat(manager.getProcessor("jmespath")).isNotNull();
        assertThat(manager.getProcessor("regex")).isNotNull();
        assertThat(manager.getProcessor("headerparam")).isNotNull();
        assertThat(manager.getProcessor("datetime")).isNotNull();
        assertThat(manager.getProcessor("conditional")).isNotNull();
        assertThat(manager.getProcessor("hash")).isNotNull();
        assertThat(manager.getProcessor("math")).isNotNull();
        
        // Verify existing processors are still registered
        assertThat(manager.getProcessor("jsonpath")).isNotNull();
        assertThat(manager.getProcessor("xpath")).isNotNull();
        assertThat(manager.getProcessor("random")).isNotNull();
    }
    
    @Test
    void testNewProcessorsSupportTemplates() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager();
        
        // Test that new processors recognize their templates
        assertThat(manager.canProcess("${jmespath:user.name:result}")).isTrue();
        assertThat(manager.canProcess("${regex:pattern:result}")).isTrue();
        assertThat(manager.canProcess("${header:Content-Type:result}")).isTrue();
        assertThat(manager.canProcess("${param:id:result}")).isTrue();
        assertThat(manager.canProcess("${cookie:session:result}")).isTrue();
        assertThat(manager.canProcess("${datetime:now:yyyy-MM-dd:result}")).isTrue();
        assertThat(manager.canProcess("${if:status==200:success:error:result}")).isTrue();
        assertThat(manager.canProcess("${hash:MD5:input:result}")).isTrue();
        assertThat(manager.canProcess("${math:1+1:result}")).isTrue();
    }
    
    @Test
    void testProcessorNames() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager();
        
        // Test processor names
        assertThat(manager.getProcessor("jmespath").getName()).isEqualTo("jmespath");
        assertThat(manager.getProcessor("regex").getName()).isEqualTo("regex");
        assertThat(manager.getProcessor("headerparam").getName()).isEqualTo("headerparam");
        assertThat(manager.getProcessor("datetime").getName()).isEqualTo("datetime");
        assertThat(manager.getProcessor("conditional").getName()).isEqualTo("conditional");
        assertThat(manager.getProcessor("hash").getName()).isEqualTo("hash");
        assertThat(manager.getProcessor("math").getName()).isEqualTo("math");
    }
    
    @Test
    void testConstructorWithoutDefaults() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
        
        // Should have no processors registered
        assertThat(manager.getProcessors()).isEmpty();
        assertThat(manager.canProcess("${jmespath:user.name}")).isFalse();
        assertThat(manager.canProcess("${jsonpath:user.name}")).isFalse();
    }
    
    @Test
    void testRegisterDefaultProcessorsMethod() {
        ExchangeTemplateManager manager = new ExchangeTemplateManager(false);
        assertThat(manager.getProcessors()).isEmpty();
        
        manager.registerDefaultProcessors();
        
        // Now should have all processors
        assertThat(manager.getProcessors()).hasSize(10); // 3 original + 7 new
        assertThat(manager.getProcessor("jmespath")).isNotNull();
        assertThat(manager.getProcessor("jsonpath")).isNotNull();
    }
}