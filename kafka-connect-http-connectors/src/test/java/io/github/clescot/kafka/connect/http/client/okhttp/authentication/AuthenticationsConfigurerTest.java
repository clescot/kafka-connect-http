package io.github.clescot.kafka.connect.http.client.okhttp.authentication;


import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AuthenticationsConfigurerTest {

    @Test
    void test_constructor_with_null(){
        Assertions.assertThrows(NullPointerException.class,()->new AuthenticationsConfigurer(null));
    }

    @Test
    void test_constructor_with_empty_list(){
        Assertions.assertThrows(IllegalArgumentException.class,()->new AuthenticationsConfigurer(Lists.newArrayList()));
    }
}