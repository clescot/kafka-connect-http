package io.github.clescot.kafka.connect.sse.client.okhttp;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * What an SSE configuration asks its server for when it starts with no committed offset: its Resume
 * Policy. Once an offset is committed the committed offset wins, so a configuration follows its
 * policy at most once, at its cold start.
 */
public enum SseResumePolicy {

  /**
   * Ask nothing: the server's own resume policy applies, and events published before the first
   * commit are not recoverable. This is the default, and the behaviour of a configuration that
   * predates the setting.
   */
  FROM_COMMITTED_OFFSET(SseConfigDefinition.RESUME_POLICY_FROM_COMMITTED_OFFSET),

  /**
   * Ask the server for the whole stream, sending the configured event id sentinel as {@code
   * Last-Event-ID}. A server that does not recognise the sentinel resumes wherever its own policy
   * says.
   */
  FROM_START(SseConfigDefinition.RESUME_POLICY_FROM_START),

  /**
   * Refuse to start the task rather than let its streams begin at a position nobody chose. The
   * offset gap becomes an operator's problem in the log instead of silent data loss.
   */
  FAIL(SseConfigDefinition.RESUME_POLICY_FAIL);

  private final String settingValue;

  SseResumePolicy(String settingValue) {
    this.settingValue = settingValue;
  }

  /**
   * Reads the policy of a configuration from its settings, defaulting to {@link
   * #FROM_COMMITTED_OFFSET} when the setting is absent.
   *
   * @param settings the configuration's settings, without the {@code config.<id>.} prefix
   * @return the configured policy
   * @throws IllegalArgumentException when the setting carries an unknown value
   */
  public static SseResumePolicy fromSettings(Map<String, String> settings) {
    String value =
        settings.getOrDefault(
            SseConfigDefinition.RESUME_POLICY, FROM_COMMITTED_OFFSET.settingValue);
    return Stream.of(values())
        .filter(policy -> policy.settingValue.equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "'"
                        + SseConfigDefinition.RESUME_POLICY
                        + "' value '"
                        + value
                        + "' is unknown, expected one of "
                        + Stream.of(values())
                            .map(SseResumePolicy::settingValue)
                            .collect(Collectors.joining(", "))));
  }

  /** Returns the value this policy carries in {@code config.<id>.resume.policy}. */
  public String settingValue() {
    return settingValue;
  }
}
