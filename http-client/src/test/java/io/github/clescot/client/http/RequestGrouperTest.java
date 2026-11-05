package io.github.clescot.client.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import io.github.clescot.core.http.HttpRequest;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

class RequestGrouperTest {

    private static RequestGrouper<String> grouper(int messageLimit, int bodyLimit) {
        return new RequestGrouper<>(
                "test", request -> true, "", "", "", messageLimit, bodyLimit);
    }

    private static Pair<String, HttpRequest> entry(String key, String body) {
        HttpRequest request = new HttpRequest("http://match.example/" + key);
        request.setBodyAsString(body);
        return Pair.of(key, request);
    }

    @Test
    void test_null_body_without_limit_is_not_grouped_as_the_literal_null() {
        List<Pair<String, HttpRequest>> grouped =
                grouper(-1, -1).group(Lists.newArrayList(entry("first", null)));

        assertThat(grouped).hasSize(1);
        assertThat(grouped.get(0).getRight().getBodyAsString()).isEmpty();
    }

    @Test
    void test_null_body_with_body_limit_does_not_throw() {
        List<Pair<String, HttpRequest>> grouped =
                grouper(-1, 3).group(Lists.newArrayList(entry("first", null), entry("second", "ok")));

        assertThat(grouped).hasSize(1);
        assertThat(grouped.get(0).getRight().getBodyAsString()).isEqualTo("ok");
    }
}
