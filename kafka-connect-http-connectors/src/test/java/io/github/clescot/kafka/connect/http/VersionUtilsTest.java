package io.github.clescot.kafka.connect.http;

import io.github.clescot.kafka.connect.VersionUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VersionUtilsTest {

    @Test
    public void test_get_version(){
        VersionUtils versionUtils = new VersionUtils();
        String version = versionUtils.getVersion();
        assertThat(version).isNotNull();
        assertThat(version).isNotEqualTo("0.0.0");
    }

}