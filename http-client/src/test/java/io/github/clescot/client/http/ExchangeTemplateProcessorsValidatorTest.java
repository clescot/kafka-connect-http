package io.github.clescot.client.http;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExchangeTemplateProcessorsValidatorTest {

    private final ExchangeTemplateProcessorsValidator validator = new ExchangeTemplateProcessorsValidator();

    @Test
    void test_null_value_is_valid() {
        assertThatCode(() -> validator.ensureValid("exchange.template.processors", null))
                .doesNotThrowAnyException();
    }

    @Test
    void test_empty_value_is_valid() {
        assertThatCode(() -> validator.ensureValid("exchange.template.processors", ""))
                .doesNotThrowAnyException();
        assertThatCode(() -> validator.ensureValid("exchange.template.processors", "   "))
                .doesNotThrowAnyException();
    }

    @Test
    void test_builtin_names_are_valid() {
        assertThatCode(() -> validator.ensureValid("exchange.template.processors", "jsonpath,random,xpath"))
                .doesNotThrowAnyException();
    }

    @Test
    void test_documented_aliases_are_valid() {
        assertThatCode(() -> validator.ensureValid("exchange.template.processors", "headerparam,conditional"))
                .doesNotThrowAnyException();
    }

    @Test
    void test_builtin_names_are_case_insensitive() {
        assertThatCode(() -> validator.ensureValid("exchange.template.processors", "JsonPath,RANDOM"))
                .doesNotThrowAnyException();
    }

    @Test
    void test_blank_entries_are_ignored() {
        assertThatCode(() -> validator.ensureValid("exchange.template.processors", " jsonpath , , random "))
                .doesNotThrowAnyException();
    }

    @Test
    void test_unknown_name_is_rejected() {
        assertThatThrownBy(() -> validator.ensureValid("exchange.template.processors", "jsonpth"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("jsonpth")
                .hasMessageContaining("jsonpath");
    }

    @Test
    void test_unknown_name_among_valid_ones_is_rejected() {
        assertThatThrownBy(() -> validator.ensureValid("exchange.template.processors", "jsonpath,invalid-processor,random"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("invalid-processor");
    }
}
