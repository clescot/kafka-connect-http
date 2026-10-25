package io.github.clescot.client.http.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RateLimitHeadersTest {

  private static Map<String, List<String>> headers(String... keyValues) {
    Map<String, List<String>> headers = Maps.newHashMap();
    for (int i = 0; i < keyValues.length; i += 2) {
      headers.put(keyValues[i], Lists.newArrayList(keyValues[i + 1]));
    }
    return headers;
  }

  @Nested
  class Parse {

    @Test
    void test_exhausted_service_limit_with_effective_window() {
      RateLimitHeaders.Snapshot snapshot =
          RateLimitHeaders.parse(headers("RateLimit", "\"default\";r=0;t=30"));
      assertThat(snapshot.serviceLimits()).hasSize(1);
      RateLimitHeaders.ServiceLimit limit = snapshot.serviceLimits().get(0);
      assertThat(limit.policy()).isEqualTo("default");
      assertThat(limit.remaining()).isZero();
      assertThat(limit.effectiveWindowSeconds()).isEqualTo(30L);
    }

    @Test
    void test_header_name_is_case_insensitive() {
      RateLimitHeaders.Snapshot snapshot =
          RateLimitHeaders.parse(headers("ratelimit", "\"default\";r=0;t=30"));
      assertThat(snapshot.serviceLimits()).hasSize(1);
    }

    @Test
    void test_policy_window_is_read_from_the_matching_policy() {
      Map<String, List<String>> headers =
          headers(
              "RateLimit-Policy", "\"daily\";q=1000;w=86400",
              "RateLimit", "\"daily\";r=0");
      RateLimitHeaders.Snapshot snapshot = RateLimitHeaders.parse(headers);
      assertThat(snapshot.serviceLimits().get(0).effectiveWindowSeconds()).isNull();
      assertThat(snapshot.advertisedWindowSeconds(snapshot.serviceLimits().get(0)))
          .contains(86400L);
    }

    @Test
    void test_multiple_policies_and_quota_units() {
      Map<String, List<String>> headers =
          headers(
              "RateLimit-Policy",
              "\"requests\";q=50;w=60,\"bytes\";q=1000;qu=\"content-bytes\";w=60",
              "RateLimit",
              "\"requests\";r=3;t=60,\"bytes\";r=0;t=60");
      RateLimitHeaders.Snapshot snapshot = RateLimitHeaders.parse(headers);
      assertThat(snapshot.serviceLimits()).hasSize(2);
      assertThat(snapshot.countsRequestAdmissions(snapshot.serviceLimits().get(0))).isTrue();
      assertThat(snapshot.countsRequestAdmissions(snapshot.serviceLimits().get(1))).isFalse();
    }

    @Test
    void test_malformed_items_are_ignored() {
      Map<String, List<String>> headers =
          headers("RateLimit", "not a valid item,\"default\";r=abc,\"ok\";r=0;t=5");
      RateLimitHeaders.Snapshot snapshot = RateLimitHeaders.parse(headers);
      assertThat(snapshot.serviceLimits()).hasSize(1);
      assertThat(snapshot.serviceLimits().get(0).policy()).isEqualTo("ok");
    }

    @Test
    void test_item_without_r_is_ignored() {
      assertThat(RateLimitHeaders.parse(headers("RateLimit", "\"default\"")).serviceLimits())
          .isEmpty();
      assertThat(RateLimitHeaders.parse(headers("RateLimit", "\"default\";t=5")).serviceLimits())
          .isEmpty();
    }

    @Test
    void test_partition_keys_and_unknown_parameters_are_skipped() {
      Map<String, List<String>> headers =
          headers(
              "RateLimit",
              "\"default\";r=0;t=10;pk=:cHsdsRa894==:;vendor-thing=42,\"second\";r=0;t=20");
      RateLimitHeaders.Snapshot snapshot = RateLimitHeaders.parse(headers);
      assertThat(snapshot.serviceLimits()).hasSize(2);
      assertThat(snapshot.serviceLimits().get(0).effectiveWindowSeconds()).isEqualTo(10L);
      assertThat(snapshot.serviceLimits().get(1).effectiveWindowSeconds()).isEqualTo(20L);
    }

    @Test
    void test_quoted_policy_names_and_empty_headers() {
      assertThat(RateLimitHeaders.parse(Maps.newHashMap()).serviceLimits()).isEmpty();
      assertThat(RateLimitHeaders.parse(null).serviceLimits()).isEmpty();
      RateLimitHeaders.Snapshot snapshot =
          RateLimitHeaders.parse(headers("RateLimit", "\"weird name\";r=0;t=3"));
      assertThat(snapshot.serviceLimits().get(0).policy()).isEqualTo("weird name");
    }

    @Test
    void test_t_zero_is_kept_as_zero() {
      RateLimitHeaders.ServiceLimit limit =
          RateLimitHeaders.parse(headers("RateLimit", "\"default\";r=0;t=0"))
              .serviceLimits()
              .get(0);
      assertThat(limit.effectiveWindowSeconds()).isZero();
    }

    @Test
    void test_malformed_t_is_treated_as_absent() {
      RateLimitHeaders.ServiceLimit limit =
          RateLimitHeaders.parse(headers("RateLimit", "\"default\";r=0;t=soon"))
              .serviceLimits()
              .get(0);
      assertThat(limit.effectiveWindowSeconds()).isNull();
    }

    @Test
    void test_negative_values_are_ignored() {
      assertThat(RateLimitHeaders.parse(headers("RateLimit", "\"default\";r=-1;t=5")).serviceLimits())
          .isEmpty();
    }
  }
}
