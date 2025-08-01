package io.github.clescot.kafka.connect.http.client.okhttp.authentication;


import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class AuthenticationsConfigurerTest {

    @Test
    void test_constructor_with_null(){
        Assertions.assertThrows(NullPointerException.class,()->new AuthenticationsConfigurer(null));
    }

    @Test
    void test_constructor_with_empty_list(){
        List<AuthenticationConfigurer> emptyList = Lists.newArrayList();
        Assertions.assertThrows(IllegalArgumentException.class,()->new AuthenticationsConfigurer(emptyList));
    }
}