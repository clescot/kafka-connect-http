package io.github.clescot.verification.grouper;

import io.github.clescot.client.http.RequestGrouper;
import io.github.clescot.core.http.HttpRequest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Exhaustive check of the promise {@link RequestGrouper#group(List)} makes: every request handed to
 * the grouper comes out of it exactly once - inside one aggregated body or on its own - each
 * aggregate goes to a matching request's URL, and the call terminates.
 *
 * <p>It calls the real grouper. The input space is finite: every sequence of at most {@link
 * #MAX_LENGTH} entries drawn from {@link #KINDS}, for every pair in {@link #MESSAGE_LIMITS} x
 * {@link #BODY_LIMITS}.
 *
 * <p>Exit code: 0 when the promise holds for every case, 1 when at least one case breaks it, 4 on
 * a harness error.
 */
public final class GrouperChecker {

  /** An entry handed to the grouper: a body token and whether the grouper's predicate accepts it. */
  record Kind(String token, boolean matches) {}

  /**
   * The four request shapes the check enumerates. Tokens never contain the separator, so a grouped
   * body can be split back into the bodies that went into it.
   */
  static final List<Kind> KINDS =
      List.of(
          new Kind("a", true), // matching, short body
          new Kind("bb", true), // matching, longer body, to cross body.limit
          new Kind("x", false), // non-matching, short body
          new Kind("y", false)); // non-matching, longer body

  /** -1 means "no limit", which is what RequestGrouperFactory uses when the setting is absent. */
  static final int[] MESSAGE_LIMITS = {-1, 1, 2, 3};

  static final int[] BODY_LIMITS = {-1, 2, 3, 5};

  static final int MAX_LENGTH = 4;
  static final String SEPARATOR = "|";
  static final String START = "[";
  static final String END = "]";
  static final int MAX_REPORTED = 12;

  public static void main(String[] args) {
    long cases = 0;
    List<String> violations = new ArrayList<>();
    for (int length = 1; length <= MAX_LENGTH; length++) {
      for (List<Kind> entries : sequencesOfLength(length)) {
        for (int messageLimit : MESSAGE_LIMITS) {
          for (int bodyLimit : BODY_LIMITS) {
            cases++;
            String violation = check(entries, messageLimit, bodyLimit);
            if (violation != null) {
              violations.add(violation);
            }
          }
        }
      }
    }

    System.out.printf(
        "grouper-check: %d cases = every sequence of 1..%d entries over %d kinds (%s) x %d message.limit x %d body.limit%n",
        cases, MAX_LENGTH, KINDS.size(), tokens(), MESSAGE_LIMITS.length, BODY_LIMITS.length);
    if (violations.isEmpty()) {
      System.out.println("grouper-check: no violation - every request came out exactly once");
      System.exit(0);
    }
    Map<String, Long> byCause =
        violations.stream()
            .collect(
                Collectors.groupingBy(
                    v -> v.substring(v.indexOf("-> ") + 3, v.indexOf(':', v.indexOf("-> ") + 3)),
                    LinkedHashMap::new,
                    Collectors.counting()));
    System.out.println("grouper-check: " + violations.size() + " violating case(s) " + byCause);
    violations.stream().limit(MAX_REPORTED).forEach(v -> System.out.println("  " + v));
    if (violations.size() > MAX_REPORTED) {
      System.out.println("  ... " + (violations.size() - MAX_REPORTED) + " more");
    }
    System.exit(1);
  }

  /**
   * Runs one case. Returns null when the grouper kept its promise, or a one-line description of
   * what it did instead.
   */
  static String check(List<Kind> kinds, int messageLimit, int bodyLimit) {
    List<Pair<String, HttpRequest>> entries = new ArrayList<>();
    Set<String> matchingUrls = new java.util.LinkedHashSet<>();
    for (int i = 0; i < kinds.size(); i++) {
      Kind kind = kinds.get(i);
      String url = (kind.matches() ? "http://match.example/" : "http://other.example/") + i;
      HttpRequest request = new HttpRequest(url);
      request.setBodyAsString(kind.token());
      entries.add(Pair.of("record-" + i, request));
      if (kind.matches()) {
        matchingUrls.add(url);
      }
    }

    List<Pair<String, HttpRequest>> grouped;
    try {
      grouped =
          new RequestGrouper<String>(
                  "check", GrouperChecker::matches, SEPARATOR, START, END, messageLimit, bodyLimit)
              .group(entries);
    } catch (StackOverflowError e) {
      return describe(kinds, messageLimit, bodyLimit) + " -> does not terminate: StackOverflowError";
    } catch (RuntimeException e) {
      return describe(kinds, messageLimit, bodyLimit) + " -> threw: " + e;
    }

    Map<String, Integer> expected = count(kinds.stream().map(Kind::token).toList());
    List<String> observedParts = new ArrayList<>();
    for (Pair<String, HttpRequest> request : grouped) {
      String body = request.getRight().getBodyAsString();
      if (body.startsWith(START) && body.endsWith(END)) {
        String inner = body.substring(START.length(), body.length() - END.length());
        List<String> parts = inner.isEmpty() ? List.of() : List.of(inner.split("\\|", -1));
        if (parts.isEmpty()) {
          return describe(kinds, messageLimit, bodyLimit)
              + " -> invents an empty request: a group with no body sent to "
              + request.getRight().getUrl();
        }
        observedParts.addAll(parts);
        if (!matchingUrls.contains(request.getRight().getUrl())) {
          return describe(kinds, messageLimit, bodyLimit)
              + " -> wrong destination: group '" + inner + "' sent to " + request.getRight().getUrl();
        }
        if (parts.stream().anyMatch(part -> !isMatchingToken(part))) {
          return describe(kinds, messageLimit, bodyLimit)
              + " -> group swallowed a non-matching body: '" + inner + "'";
        }
      } else {
        observedParts.add(body);
      }
    }

    Map<String, Integer> actual = count(observedParts);
    if (!expected.equals(actual)) {
      List<String> lost = new ArrayList<>();
      expected.forEach(
          (token, n) -> {
            int got = actual.getOrDefault(token, 0);
            if (got < n) lost.add(token + " x" + (n - got));
          });
      List<String> duplicated = new ArrayList<>();
      actual.forEach(
          (token, n) -> {
            int want = expected.getOrDefault(token, 0);
            if (n > want) duplicated.add(token + " x" + (n - want));
          });
      String cause =
          lost.isEmpty() ? "duplicates a request" : duplicated.isEmpty() ? "loses a request" : "loses and duplicates";
      return describe(kinds, messageLimit, bodyLimit)
          + " -> "
          + cause
          + ": in "
          + tokens(expected)
          + " out "
          + tokens(actual);
    }
    return null;
  }

  private static boolean isMatchingToken(String token) {
    return KINDS.stream().anyMatch(kind -> kind.matches() && kind.token().equals(token));
  }

  private static boolean matches(HttpRequest request) {
    return request.getUrl().startsWith("http://match");
  }

  private static Map<String, Integer> count(List<String> values) {
    Map<String, Integer> counts = new java.util.TreeMap<>();
    for (String value : values) {
      counts.merge(value, 1, Integer::sum);
    }
    return counts;
  }

  private static String tokens() {
    return KINDS.stream()
        .map(kind -> (kind.matches() ? "M:" : "N:") + kind.token())
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.joining(","));
  }

  private static String tokens(Map<String, Integer> counts) {
    return counts.toString();
  }

  private static String describe(List<Kind> kinds, int messageLimit, int bodyLimit) {
    return "entries="
        + kinds.stream().map(kind -> (kind.matches() ? "M(" : "N(") + kind.token() + ")").toList()
        + " message.limit="
        + messageLimit
        + " body.limit="
        + bodyLimit;
  }

  private static List<List<Kind>> sequencesOfLength(int length) {
    List<List<Kind>> sequences = new ArrayList<>();
    build(new ArrayList<>(), length, sequences);
    return sequences;
  }

  private static void build(List<Kind> prefix, int length, List<List<Kind>> out) {
    if (prefix.size() == length) {
      out.add(List.copyOf(prefix));
      return;
    }
    for (Kind kind : KINDS) {
      prefix.add(kind);
      build(prefix, length, out);
      prefix.remove(prefix.size() - 1);
    }
  }
}
