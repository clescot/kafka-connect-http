package io.github.clescot.client.http.ratelimit;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Tolerant parser for the server-advertised quota fields defined by the IETF
 * draft-ietf-httpapi-ratelimit-headers: {@code RateLimit-Policy} (the advertised policy) and
 * {@code RateLimit} (the current service limit).
 *
 * <p>Malformed items and unknown parameters are ignored, as the specification requires.
 * Partition keys ({@code pk}) are parsed only far enough to be skipped.
 */
public final class RateLimitHeaders {

    public static final String RATE_LIMIT = "RateLimit";
    public static final String RATE_LIMIT_POLICY = "RateLimit-Policy";
    public static final String DEFAULT_QUOTA_UNIT = "requests";
    public static final String CONCURRENT_REQUESTS_QUOTA_UNIT = "concurrent-requests";

    private RateLimitHeaders() {}

    /** one quota policy item of the {@code RateLimit-Policy} field. */
    public record QuotaPolicy(String policy, String quotaUnit, Long windowSeconds) {}

    /** one service limit item of the {@code RateLimit} field. */
    public record ServiceLimit(String policy, long remaining, Long effectiveWindowSeconds) {}

    /** the parsed view of both fields. */
    public static final class Snapshot {
        private final List<ServiceLimit> serviceLimits;
        private final List<QuotaPolicy> quotaPolicies;

        public Snapshot(List<ServiceLimit> serviceLimits, List<QuotaPolicy> quotaPolicies) {
            this.serviceLimits = Lists.newArrayList(serviceLimits);
            this.quotaPolicies = Lists.newArrayList(quotaPolicies);
        }

        public static Snapshot empty() {
            return new Snapshot(Lists.newArrayList(), Lists.newArrayList());
        }

        public List<ServiceLimit> serviceLimits() {
            return serviceLimits;
        }

        public List<QuotaPolicy> quotaPolicies() {
            return quotaPolicies;
        }

        /** the policy advertised for the given policy name, if any. */
        public Optional<QuotaPolicy> policy(String policyName) {
            if (policyName == null) {
                return Optional.empty();
            }
            Optional<QuotaPolicy> exact =
                    quotaPolicies.stream().filter(policy -> policyName.equals(policy.policy())).findFirst();
            if (exact.isPresent()) {
                return exact;
            }
            return quotaPolicies.stream()
                    .filter(policy -> policyName.equalsIgnoreCase(policy.policy()))
                    .findFirst();
        }

        /** the quota unit of a service limit: its matching policy's unit, or the default unit. */
        public String quotaUnit(ServiceLimit serviceLimit) {
            return policy(serviceLimit.policy())
                    .map(QuotaPolicy::quotaUnit)
                    .filter(unit -> unit != null && !unit.isBlank())
                    .orElse(DEFAULT_QUOTA_UNIT);
        }

        /** true when the service limit's quota unit counts request admissions. */
        public boolean countsRequestAdmissions(ServiceLimit serviceLimit) {
            String unit = quotaUnit(serviceLimit);
            return DEFAULT_QUOTA_UNIT.equals(unit) || CONCURRENT_REQUESTS_QUOTA_UNIT.equals(unit);
        }

        /** the window advertised for a service limit: its {@code t}, else its policy's {@code w}. */
        public Optional<Long> advertisedWindowSeconds(ServiceLimit serviceLimit) {
            if (serviceLimit.effectiveWindowSeconds() != null) {
                return Optional.of(serviceLimit.effectiveWindowSeconds());
            }
            return policy(serviceLimit.policy())
                    .map(QuotaPolicy::windowSeconds)
                    .filter(window -> window != null);
        }
    }

    public static Snapshot parse(Map<String, List<String>> headers) {
        if (headers == null) {
            return Snapshot.empty();
        }
        List<QuotaPolicy> policies = Lists.newArrayList();
        for (String item : splitTopLevel(joinValues(headerValues(headers, RATE_LIMIT_POLICY)), ',')) {
            parsePolicy(item).ifPresent(policies::add);
        }
        List<ServiceLimit> serviceLimits = Lists.newArrayList();
        for (String item : splitTopLevel(joinValues(headerValues(headers, RATE_LIMIT)), ',')) {
            parseServiceLimit(item).ifPresent(serviceLimits::add);
        }
        return new Snapshot(serviceLimits, policies);
    }

    private static Optional<QuotaPolicy> parsePolicy(String item) {
        if (item == null || item.isBlank()) {
            return Optional.empty();
        }
        List<String> parts = splitTopLevel(item, ';');
        String policyName = itemName(parts.get(0));
        if (policyName == null) {
            return Optional.empty();
        }
        Map<String, String> parameters = parameters(parts);
        if (nonNegativeInteger(parameters.get("q")).isEmpty()) {
            // "q" is required on a policy item
            return Optional.empty();
        }
        String quotaUnit = parameters.getOrDefault("qu", DEFAULT_QUOTA_UNIT);
        Long window = positiveInteger(parameters.get("w")).orElse(null);
        return Optional.of(new QuotaPolicy(policyName, quotaUnit, window));
    }

    private static Optional<ServiceLimit> parseServiceLimit(String item) {
        if (item == null || item.isBlank()) {
            return Optional.empty();
        }
        List<String> parts = splitTopLevel(item, ';');
        String policyName = itemName(parts.get(0));
        if (policyName == null) {
            return Optional.empty();
        }
        Map<String, String> parameters = parameters(parts);
        Optional<Long> remaining = nonNegativeInteger(parameters.get("r"));
        if (remaining.isEmpty()) {
            // "r" is required on a service limit item
            return Optional.empty();
        }
        // an absent or malformed "t" falls back to the matching policy window, then to the
        // configured default delay; an explicit "t=0" means "no pause".
        Long effectiveWindow = nonNegativeInteger(parameters.get("t")).orElse(null);
        return Optional.of(new ServiceLimit(policyName, remaining.get(), effectiveWindow));
    }

    private static List<String> headerValues(Map<String, List<String>> headers, String headerName) {
        List<String> values = Lists.newArrayList();
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(headerName) && entry.getValue() != null) {
                values.addAll(entry.getValue());
            }
        }
        return values;
    }

    private static String joinValues(List<String> values) {
        return String.join(",", values);
    }

    /**
     * Splits on the given delimiter, ignoring delimiters inside quoted strings and byte
     * sequences.
     */
    static List<String> splitTopLevel(String value, char delimiter) {
        List<String> parts = Lists.newArrayList();
        if (value == null) {
            return parts;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean inByteSequence = false;
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (inQuotes) {
                current.append(character);
                if (character == '\\' && i + 1 < value.length()) {
                    current.append(value.charAt(++i));
                } else if (character == '"') {
                    inQuotes = false;
                }
                continue;
            }
            if (inByteSequence) {
                current.append(character);
                if (character == ':') {
                    inByteSequence = false;
                }
                continue;
            }
            if (character == '"') {
                inQuotes = true;
                current.append(character);
            } else if (character == ':') {
                inByteSequence = true;
                current.append(character);
            } else if (character == delimiter) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(character);
            }
        }
        parts.add(current.toString());
        return parts;
    }

    private static Map<String, String> parameters(List<String> itemParts) {
        Map<String, String> parameters = new java.util.LinkedHashMap<>();
        for (int i = 1; i < itemParts.size(); i++) {
            String parameter = itemParts.get(i);
            if (parameter == null) {
                continue;
            }
            int separator = parameter.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            String key = parameter.substring(0, separator).trim().toLowerCase(Locale.ROOT);
            String value = unquote(parameter.substring(separator + 1));
            if (!key.isEmpty() && !value.isEmpty()) {
                parameters.put(key, value);
            }
        }
        return parameters;
    }

    private static String itemName(String token) {
        if (token == null) {
            return null;
        }
        String name = unquote(token);
        if (name.isEmpty() || name.indexOf('=') >= 0) {
            return null;
        }
        return name;
    }

    private static String unquote(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            String inner = trimmed.substring(1, trimmed.length() - 1);
            return inner.replace("\\\"", "\"").replace("\\\\", "\\");
        }
        return trimmed;
    }

    private static Optional<Long> nonNegativeInteger(String value) {
        if (value == null) {
            return Optional.empty();
        }
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed >= 0 ? Optional.of(parsed) : Optional.empty();
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static Optional<Long> positiveInteger(String value) {
        return nonNegativeInteger(value).filter(integer -> integer > 0);
    }
}
