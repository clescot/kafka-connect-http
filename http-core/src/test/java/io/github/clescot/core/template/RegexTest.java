package io.github.clescot.core.template;

import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class RegexTest {

    @Test
    void testJsonPathRegex() {
        Pattern pattern = Pattern.compile("\\$\\{jsonpath:(.*?)\\}");
        String template = "${jsonpath:$.response.statusCode}";
        
        Matcher matcher = pattern.matcher(template);
        
        boolean found = matcher.find();
        assertThat(found).isTrue();
        assertThat(matcher.groupCount()).isEqualTo(1);
        assertThat(matcher.group(1)).isEqualTo("$.response.statusCode");
        
        System.out.println("Pattern matches: " + found);
        System.out.println("Group count: " + matcher.groupCount());
        System.out.println("Group 1: " + matcher.group(1));
    }
}