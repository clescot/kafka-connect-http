#!/usr/bin/env bash
#
# Exhaustive check that the HTTP sink's RequestGrouper keeps every pending
# request exactly once, and terminates.
#
#   bash .agents/skills/verify-request-grouper-preserves-requests/scripts/run-check.sh
#
# It builds http-client + http-core, compiles checker/GrouperChecker.java
# against the real RequestGrouper, runs every case in the finite input space and
# writes the result to results/check.txt.
#
# Exit codes: 0 the promise holds, 1 at least one case breaks it, 4 the check
# could not run (build, classpath, or the grouper's API moved).
#
set -uo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$SKILL_DIR/../../.." && pwd)"
RESULTS_DIR="$SKILL_DIR/results"
CHECKER="$SKILL_DIR/checker/GrouperChecker.java"
MAIN="io.github.clescot.verification.grouper.GrouperChecker"
GROUPER="$REPO_ROOT/http-client/src/main/java/io/github/clescot/client/http/RequestGrouper.java"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/grouper-check.XXXXXX")"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT
mkdir -p "$RESULTS_DIR"

fail() { echo "run-check: $*" >&2; exit 4; }

command -v mvn >/dev/null 2>&1 || fail "mvn not found (the check builds http-client with Maven)"
command -v javac >/dev/null 2>&1 || fail "javac not found (a JDK 17+ is required)"
[ -f "$GROUPER" ] || fail "RequestGrouper.java not found at $GROUPER: the grouper moved, update scripts/run-check.sh and checker/GrouperChecker.java"

echo "run-check: building http-client (installs the http-core snapshot in the local Maven repository)"
mvn -q -pl http-client -am -DskipTests install >"$WORK/build.log" 2>&1 || {
  tail -20 "$WORK/build.log" >&2
  fail "cannot build http-client (see the log above). The check reads the real class, so the module has to compile."
}

mvn -q -pl http-client dependency:build-classpath -Dmdep.outputFile="$WORK/deps.txt" >"$WORK/cp.log" 2>&1 || {
  tail -20 "$WORK/cp.log" >&2
  fail "cannot resolve the http-client classpath (is the local Maven repository populated?)"
}

CP="$REPO_ROOT/http-client/target/classes:$REPO_ROOT/http-core/target/classes:$(cat "$WORK/deps.txt")"

echo "run-check: compiling the checker against the real RequestGrouper"
javac -cp "$CP" -d "$WORK/classes" "$CHECKER" 2>"$WORK/javac.log" || {
  cat "$WORK/javac.log" >&2
  fail "the checker no longer compiles: RequestGrouper's package, constructor or group(List) signature changed. Update checker/GrouperChecker.java to the new API - do not relax the property."
}

echo "run-check: running every case (the grouper is called in-process, one JVM)"
java -Xss512k -cp "$WORK/classes:$SKILL_DIR/checker:$CP" "$MAIN" | tee "$RESULTS_DIR/check.txt"
STATUS="${PIPESTATUS[0]}"

echo
case "$STATUS" in
  0)
    echo "VERDICT: PROMISE HOLDS - every pending request came out of the grouper exactly once,"
    echo "         on a matching URL, and every case terminated."
    exit 0
    ;;
  1)
    echo "VERDICT: PROMISE BROKEN - at least one case loses, duplicates, invents or misroutes a"
    echo "         request, or does not terminate. These are findings about the code: read"
    echo "         results/check.txt, do not weaken the checker, and see 'Findings' in SKILL.md."
    exit 1
    ;;
  *)
    echo "VERDICT: ERROR - the checker itself failed (exit $STATUS); see results/check.txt."
    exit 4
    ;;
esac
