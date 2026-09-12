#!/usr/bin/env bash
#
# upstream-watch — mechanize the Spark Connect currency check.
#
# Until now, drift was only noticed when *we* happened to bump the pin: the
# 2026-09-11 discovery that 4.2.0 existed (and that apache/spark master had
# grown SCD_TYPE_2) was done by hand. This script is that afternoon, automated,
# and runnable locally:
#
#   1. read the pinned spark-connect-common version out of build.sbt;
#   2. ask Maven Central which versions are newer (stable preferred; previews
#      and RCs are reported separately, and only used as the candidate when
#      nothing stable is newer);
#   3. re-render the conformance inventory against the candidate artifact
#      (`SBT_OPTS=-Dsdp.connect.common.version=…`, the launch override build.sbt
#      reads — see the note at the sbt invocation: a command-line -D is NOT
#      forwarded to the sbt 2 server, and a warm server ignores it outright)
#      and diff it against the committed snapshot — that diff IS the wire
#      change report;
#   4. fetch pipelines.proto from apache/spark master and list message/field
#      names the committed inventory has never seen — the earliest possible
#      intelligence about the NEXT release (this is how SCD_TYPE_2 was spotted);
#   5. ask Central whether `iceberg-spark-runtime-4.2_2.13` has been RELEASED.
#      The AUTO CDC e2e (`IcebergAutoCdcE2eSpec`, rows CDC-5/CDC-6) can only run
#      against a merge-capable table format, and Iceberg is the only OSS one —
#      but it has no 4.2 release, so that suite pins an Apache SNAPSHOT jar.
#      Snapshots are mutable and eventually swept, so the day a release exists
#      is the day the pin must be swapped: CRITICAL PATH, not demo polish;
#   6. open-or-update exactly ONE GitHub issue carrying all of it.
#
# What it deliberately does NOT do: bump the pin. protobuf-java and grpc must
# match the chosen Spark release's own pom (DECISIONS D2); that is a human
# judgment, so this reports and a human decides.
#
# Usage:
#   scripts/upstream-watch.sh [options]
#     --pin X.Y.Z      pretend build.sbt pins this (for testing the drift path)
#     --candidate V    investigate this version instead of the newest one Central
#                      offers (for testing, and for "what would 4.1.3 change?")
#     --file-issue     actually open/update the GitHub issue (needs `gh` + auth).
#                      Without it, the issue body is printed to stdout only.
#     --no-inventory   skip the sbt inventory re-render (version + proto intel only)
#     --help
#
# Exit codes: 0 = ran fine (drift or no drift). Non-zero = the check itself broke
# (no network, unparseable metadata, …) — that is a real CI failure, because a
# watch that silently stops watching is worse than no watch.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_SBT="$REPO_ROOT/build.sbt"
SNAPSHOT="$REPO_ROOT/sdp/src/test/resources/spark-connect-inventory.txt"

METADATA_URL="https://repo1.maven.org/maven2/org/apache/spark/spark-connect-common_2.13/maven-metadata.xml"
MASTER_PROTO_URL="https://raw.githubusercontent.com/apache/spark/master/sql/connect/common/src/main/protobuf/spark/connect/pipelines.proto"
ISSUE_LABEL="upstream-watch"

# The second watched artifact: the AUTO CDC e2e's server-side fixture. It is NOT
# a build dependency (it never appears in libraryDependencies) — it is the jar
# `IcebergAutoCdcE2eSpec` mounts into the test container, and today it can only
# be an Apache SNAPSHOT because no Spark-4.2 Iceberg runtime is released.
ICEBERG_ARTIFACT="org.apache.iceberg:iceberg-spark-runtime-4.2_2.13"
ICEBERG_METADATA_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.2_2.13/maven-metadata.xml"
ICEBERG_PIN_FILE="$REPO_ROOT/sdp-it/src/test/scala/dev/sdp/connect/SparkConnectTestServer.scala"

PIN_OVERRIDE=""
CANDIDATE_OVERRIDE=""
FILE_ISSUE="${SDP_WATCH_FILE_ISSUE:-false}"
RUN_INVENTORY=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pin)          PIN_OVERRIDE="${2:?--pin needs a version}"; shift 2 ;;
    --candidate)    CANDIDATE_OVERRIDE="${2:?--candidate needs a version}"; shift 2 ;;
    --file-issue)   FILE_ISSUE=true; shift ;;
    --no-inventory) RUN_INVENTORY=false; shift ;;
    --help|-h)      sed -n '2,45p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *)              echo "upstream-watch: unknown option '$1' (try --help)" >&2; exit 2 ;;
  esac
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

log()  { echo "upstream-watch: $*" >&2; }
# One line, comma-separated. `paste -sd', '` would CYCLE the two delimiter
# characters (",", " ") between items — a classic BSD/GNU paste trap.
commas() { paste -sd',' "$1" | sed 's/,/, /g'; }
fail() { echo "upstream-watch: $*" >&2; exit 1; }

# --- 1. the pin ------------------------------------------------------------
# Single source of truth in build.sbt:
#   val sparkConnectCommonVersion = sys.props.getOrElse("sdp.connect.common.version", "4.2.0")
read_pin() {
  sed -nE 's/.*sdp\.connect\.common\.version"[[:space:]]*,[[:space:]]*"([^"]+)".*/\1/p' "$BUILD_SBT" | head -1
}

# --- version algebra ------------------------------------------------------
# `sort -V` orders 4.2.0-preview1 AFTER 4.2.0, which is wrong for release
# precedence — so prereleases are never compared directly: they are reduced to
# their base version (4.2.0-preview1 -> 4.2.0) first.
is_stable()  { [[ "$1" =~ ^[0-9]+(\.[0-9]+)*$ ]]; }
base_of()    { echo "${1%%-*}"; }
ver_gt() { # ver_gt A B  → true when A > B
  [[ "$1" != "$2" ]] && [[ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" == "$1" ]]
}

PIN="${PIN_OVERRIDE:-$(read_pin)}"
[[ -n "$PIN" ]] || fail "could not read the spark-connect-common pin from $BUILD_SBT"
is_stable "$PIN" || log "note: the pin '$PIN' is itself a prerelease"
log "pinned spark-connect-common: $PIN${PIN_OVERRIDE:+ (forced via --pin)}"

# --- 2. what does Central have? -------------------------------------------
curl -sSfL --max-time 60 "$METADATA_URL" -o "$WORK/maven-metadata.xml" \
  || fail "could not fetch $METADATA_URL"
grep -oE '<version>[^<]+</version>' "$WORK/maven-metadata.xml" \
  | sed -E 's#</?version>##g' > "$WORK/all-versions.txt"
[[ -s "$WORK/all-versions.txt" ]] || fail "no <version> entries in the Central metadata"
log "Central lists $(wc -l < "$WORK/all-versions.txt" | tr -d ' ') versions"

: > "$WORK/newer-stable.txt"
: > "$WORK/newer-pre.txt"
while read -r v; do
  [[ -n "$v" ]] || continue
  if is_stable "$v"; then
    ver_gt "$v" "$PIN" && echo "$v" >> "$WORK/newer-stable.txt"
  else
    # A preview counts as "newer" only if its BASE release is newer than the
    # pin — otherwise 4.2.0-preview5 would look newer than the 4.2.0 we ship.
    ver_gt "$(base_of "$v")" "$(base_of "$PIN")" && echo "$v" >> "$WORK/newer-pre.txt"
  fi
done < "$WORK/all-versions.txt"

sort -V -o "$WORK/newer-stable.txt" "$WORK/newer-stable.txt"
sort -V -o "$WORK/newer-pre.txt" "$WORK/newer-pre.txt"

CANDIDATE=""
CANDIDATE_KIND=""
if [[ -n "$CANDIDATE_OVERRIDE" ]]; then
  CANDIDATE="$CANDIDATE_OVERRIDE"
  CANDIDATE_KIND="forced via --candidate"
elif [[ -s "$WORK/newer-stable.txt" ]]; then
  CANDIDATE="$(tail -1 "$WORK/newer-stable.txt")"
  CANDIDATE_KIND="release"
elif [[ -s "$WORK/newer-pre.txt" ]]; then
  # Nothing stable is newer, so the previews ARE the signal (spec: report them
  # separately rather than ignoring them entirely).
  CANDIDATE="$(tail -1 "$WORK/newer-pre.txt")"
  CANDIDATE_KIND="prerelease"
fi

# --- 4. master-proto intel (computed early: useful even with no drift) -----
# Crude on purpose: message/field NAMES from master's pipelines.proto that the
# committed inventory has never seen. Not a parser — a tripwire.
proto_intel() {
  if ! curl -sSfL --max-time 60 "$MASTER_PROTO_URL" -o "$WORK/pipelines.proto"; then
    echo "(could not fetch pipelines.proto from apache/spark master)"
    return 0
  fi
  local new_messages=() new_fields=() name
  while read -r name; do
    grep -q "\.${name}$" "$SNAPSHOT" || grep -q "\.${name}\b" "$SNAPSHOT" || new_messages+=("$name")
  done < <(sed -nE 's/^[[:space:]]*message[[:space:]]+([A-Za-z0-9_]+)[[:space:]]*\{.*/\1/p' "$WORK/pipelines.proto" | sort -u)
  while read -r name; do
    grep -qE "^[[:space:]]*field [0-9]+ ${name}( |$)" "$SNAPSHOT" || new_fields+=("$name")
  done < <(sed -nE 's/^[[:space:]]*(optional |repeated |required )?[A-Za-z0-9_.]+[[:space:]]+([a-z][a-z0-9_]*)[[:space:]]*=[[:space:]]*[0-9]+[[:space:]]*;.*/\2/p' "$WORK/pipelines.proto" | sort -u)
  local enum_values=() v
  while read -r v; do
    grep -qE "^[[:space:]]*value [0-9]+ ${v}$" "$SNAPSHOT" || enum_values+=("$v")
  done < <(sed -nE 's/^[[:space:]]*([A-Z][A-Z0-9_]+)[[:space:]]*=[[:space:]]*[0-9]+[[:space:]]*;.*/\1/p' "$WORK/pipelines.proto" | sort -u)

  if [[ ${#new_messages[@]} -eq 0 && ${#new_fields[@]} -eq 0 && ${#enum_values[@]} -eq 0 ]]; then
    echo "master's pipelines.proto introduces nothing the committed inventory lacks."
  else
    [[ ${#new_messages[@]} -gt 0 ]] && printf 'messages not in our inventory: %s\n' "$(IFS=', '; echo "${new_messages[*]}")"
    [[ ${#new_fields[@]}   -gt 0 ]] && printf 'fields not in our inventory:   %s\n' "$(IFS=', '; echo "${new_fields[*]}")"
    [[ ${#enum_values[@]}  -gt 0 ]] && printf 'enum values not in our inventory: %s\n' "$(IFS=', '; echo "${enum_values[*]}")"
  fi
  return 0
}

log "reading apache/spark master pipelines.proto for early intel"
proto_intel > "$WORK/proto-intel.txt" || true

# --- 5. the AUTO CDC e2e fixture: has Iceberg released a 4.2 runtime yet? ----
# Absence is the normal answer and is NOT an error: `curl -f` on a missing
# maven-metadata.xml is a 404, which here means "still unreleased, the snapshot
# pin stands". Presence is the headline — a mutable snapshot is a temporary
# arrangement and this is the trigger to end it.
ICEBERG_RELEASED=false
iceberg_section() {
  local pinned versions
  pinned="$(sed -nE 's/.*"(iceberg-spark-runtime-4\.2_2\.13-[^"]+\.jar)".*/\1/p' "$ICEBERG_PIN_FILE" | head -1)"
  [[ -n "$pinned" ]] || pinned="(could not read the pin from ${ICEBERG_PIN_FILE#"$REPO_ROOT"/})"
  echo "- watched: \`$ICEBERG_ARTIFACT\`"
  echo "- e2e fixture pin: \`$pinned\` — an Apache **snapshot**, mounted into the container by \`IcebergAutoCdcE2eSpec\` (rows CDC-5/CDC-6). Not a build dependency."
  if ! curl -sSfL --max-time 60 "$ICEBERG_METADATA_URL" -o "$WORK/iceberg-metadata.xml" 2> /dev/null; then
    echo "- Maven Central: **not yet released**, snapshot in use — no swap to make."
    return 0
  fi
  versions="$(grep -oE '<version>[^<]+</version>' "$WORK/iceberg-metadata.xml" \
    | sed -E 's#</?version>##g' | sort -V | paste -sd',' - | sed 's/,/, /g')"
  if [[ -z "$versions" ]]; then
    echo "- Maven Central: metadata exists but lists no versions — treating as **not yet released**, snapshot in use."
    return 0
  fi
  ICEBERG_RELEASED=true
  echo "- Maven Central: **RELEASED** — $versions"
  echo "- **CRITICAL PATH.** Iceberg is the only OSS table format that implements"
  echo "  \`SupportsRowLevelOperations\`, i.e. the only way AUTO CDC materializes at all"
  echo "  (rows CDC-4/CDC-5/CDC-6). Swap the pinned snapshot URL in"
  echo "  \`SparkConnectTestServer.Iceberg42\` for the release, re-run"
  echo "  \`SDP_INTEGRATION=1 sbt 'sdpIt/Test/runMain dev.sdp.connect.IcebergAutoCdcE2eSpec'\`,"
  echo "  and update the snapshot caveat in \`docs/dsl.md\` + \`docs/developing.md\`."
  echo "  This script does not swap it: the release may move more than the coordinate."
}

log "checking Maven Central for a released $ICEBERG_ARTIFACT (the AUTO CDC e2e fixture pin)"
iceberg_section > "$WORK/iceberg.txt" || true

if [[ -z "$CANDIDATE" && "$ICEBERG_RELEASED" != true ]]; then
  log "no newer spark-connect-common than $PIN — no drift."
  log "$ICEBERG_ARTIFACT: not yet released on Central, snapshot pin stands."
  echo "--- iceberg-spark-runtime-4.2 (AUTO CDC e2e fixture) ---"
  cat "$WORK/iceberg.txt"
  echo "--- master pipelines.proto intel (informational; no issue filed) ---"
  cat "$WORK/proto-intel.txt"
  exit 0
fi

# `cond && log …` would be a `set -e` landmine as a standalone statement: a false
# condition makes the AND-list return non-zero and the script would exit here.
if [[ -n "$CANDIDATE" ]]; then log "candidate: $CANDIDATE ($CANDIDATE_KIND)"; fi
if [[ "$ICEBERG_RELEASED" == true ]]; then
  log "$ICEBERG_ARTIFACT is RELEASED — the e2e snapshot pin can be swapped"
fi

# --- 3. inventory diff against the candidate ------------------------------
inventory_section() {
  if [[ -z "$CANDIDATE" ]]; then
    # Reachable when the report exists only because of the Iceberg trigger: the
    # Spark pin is current, so there is nothing to render a candidate against.
    echo "(skipped: the spark-connect-common pin is current)"
    return 0
  fi
  if [[ "$RUN_INVENTORY" != true ]]; then
    echo "(skipped: --no-inventory)"
    return 0
  fi
  if ! command -v sbt > /dev/null 2>&1; then
    echo "(skipped: sbt not on PATH)"
    return 0
  fi
  # A warm sbt server has ALREADY evaluated build.sbt, so `-Dsdp.connect.common
  # .version=…` reaches the thin client and is then ignored — the render would
  # silently be of the CURRENT pin and the diff would be empty. A false
  # all-clear is the worst possible outcome for a drift watch, so: stop any warm
  # server first, then VERIFY from sbt's own output that the override took.
  log "stopping any warm sbt server (it would cache the old pin and diff the snapshot against itself)"
  (cd "$REPO_ROOT" && sbt --batch shutdown) > /dev/null 2>&1 || true

  # SBT_OPTS, not `sbt -Dkey=val`: sbt 2's thin client takes a command-line -D
  # for ITSELF and does not forward it to the server JVM it spawns, so the
  # override would be silently lost. SBT_OPTS is passed through to the server.
  log "rendering the conformance inventory against $CANDIDATE (this builds the project)"
  # One semicolon-joined command string: sbt 2's thin client concatenates
  # separate argv entries into a single command line, so two quoted commands
  # parse as one malformed command.
  if ! (cd "$REPO_ROOT" && SBT_OPTS="${SBT_OPTS:-} -Dsdp.connect.common.version=$CANDIDATE" sbt --batch \
          "show sdp/libraryDependencies; sdp/Test/runMain dev.sdp.connect.conformance.RenderInventory $WORK/candidate-inventory.txt") \
          > "$WORK/sbt.log" 2>&1; then
    # Distinguish "the candidate breaks us" (the headline) from "the override
    # never took effect" (our own bug — must never read as no-drift).
    if ! grep -q "spark-connect-common_2.13:$CANDIDATE" "$WORK/sbt.log"; then
      OVERRIDE_FAILED=true
    fi
    echo "The project does NOT build against spark-connect-common $CANDIDATE."
    echo "That is itself the headline: a removed or renamed message breaks our encoder."
    echo
    echo '```'
    tail -40 "$WORK/sbt.log"
    echo '```'
    (cd "$REPO_ROOT" && sbt --batch shutdown) > /dev/null 2>&1 || true
    return 0
  fi
  if ! grep -q "spark-connect-common_2.13:$CANDIDATE" "$WORK/sbt.log"; then
    OVERRIDE_FAILED=true
    echo "(inventory NOT rendered: the -Dsdp.connect.common.version override did not take effect)"
    return 0
  fi
  # Leave no booby trap: the server we just started holds the CANDIDATE version
  # in its loaded build. A developer's next `sbt compile` must not silently use
  # it, so the server goes away with the script.
  (cd "$REPO_ROOT" && sbt --batch shutdown) > /dev/null 2>&1 || true
  if diff -u "$SNAPSHOT" "$WORK/candidate-inventory.txt" > "$WORK/inventory.diff"; then
    echo "No wire-surface change: the $CANDIDATE descriptor inventory is byte-identical to the committed snapshot."
  else
    local added removed
    added="$(grep -c '^+[^+]' "$WORK/inventory.diff" || true)"
    removed="$(grep -c '^-[^-]' "$WORK/inventory.diff" || true)"
    echo "Wire-surface change: $added line(s) added, $removed removed."
    echo
    echo '```diff'
    head -200 "$WORK/inventory.diff"
    echo '```'
    [[ "$(wc -l < "$WORK/inventory.diff")" -gt 200 ]] && echo "_(diff truncated at 200 lines)_"
  fi
  return 0
}

OVERRIDE_FAILED=false
inventory_section > "$WORK/inventory-section.md" || true
if [[ "$OVERRIDE_FAILED" == true ]]; then
  fail "the -Dsdp.connect.common.version override did not reach the build (sbt resolved a
  different spark-connect-common than '$CANDIDATE'). Refusing to report: an inventory rendered
  against the CURRENT pin would diff clean and read as 'no drift'. Fix the build/launch first
  (see $WORK/sbt.log — but note this temp dir is removed on exit; re-run the sbt command by hand)."
fi

# --- 5. the report + the single issue -------------------------------------
if [[ -n "$CANDIDATE" && "$ICEBERG_RELEASED" == true ]]; then
  TITLE="upstream-watch: spark-connect-common $CANDIDATE available (we pin $PIN); iceberg-spark-runtime-4.2 RELEASED"
elif [[ -n "$CANDIDATE" ]]; then
  TITLE="upstream-watch: spark-connect-common $CANDIDATE available (we pin $PIN)"
else
  TITLE="upstream-watch: iceberg-spark-runtime-4.2_2.13 RELEASED — swap the AUTO CDC e2e snapshot pin"
fi
{
  echo "Automated currency check (\`scripts/upstream-watch.sh\`). **Nothing is bumped"
  echo "automatically** — protobuf-java and grpc must match the chosen Spark release's own"
  echo "pom (DECISIONS D2), which is a human judgment."
  echo
  echo "## Version delta — spark-connect-common"
  echo
  echo "- pinned: \`$PIN\`"
  if [[ -n "$CANDIDATE" ]]; then
    echo "- candidate: \`$CANDIDATE\` ($CANDIDATE_KIND)"
  else
    echo "- candidate: none — the pin is current"
  fi
  if [[ -s "$WORK/newer-stable.txt" ]]; then
    echo "- newer releases: $(commas "$WORK/newer-stable.txt")"
  else
    echo "- newer releases: none"
  fi
  if [[ -s "$WORK/newer-pre.txt" ]]; then
    echo "- newer previews/RCs (reported, not normally used): $(commas "$WORK/newer-pre.txt")"
  fi
  echo
  echo "## AUTO CDC e2e fixture — iceberg-spark-runtime-4.2"
  echo
  cat "$WORK/iceberg.txt"
  echo
  if [[ -n "$CANDIDATE" ]]; then
    echo "## Conformance inventory diff (\`$CANDIDATE\` vs the committed snapshot)"
  else
    echo "## Conformance inventory diff"
  fi
  echo
  cat "$WORK/inventory-section.md"
  echo
  echo "## apache/spark master \`pipelines.proto\` (early intel)"
  echo
  cat "$WORK/proto-intel.txt"
  echo
  if [[ -n "$CANDIDATE" ]]; then
    echo "## If we take spark-connect-common $CANDIDATE"
    echo
    echo "1. match \`protobuf-java\` + \`grpc\` to the release's pom, by hand;"
    echo "2. bump \`sparkConnectCommonVersion\` in build.sbt;"
    echo "3. regenerate the snapshot (\`sdp/Test/runMain dev.sdp.connect.conformance.RenderInventory sdp/src/test/resources/spark-connect-inventory.txt\`) and review that diff;"
    echo "4. re-triage any new \`Relation\`/\`Expression\` entries in \`ConnectTiers\`."
    echo
  fi
  if [[ "$ICEBERG_RELEASED" == true ]]; then
    echo "## Swapping the Iceberg pin"
    echo
    echo "1. replace the snapshot URL in \`SparkConnectTestServer.Iceberg42\` with the released jar;"
    echo "2. \`SDP_INTEGRATION=1 sbt 'sdpIt/Test/runMain dev.sdp.connect.IcebergAutoCdcE2eSpec'\` — CDC-5/CDC-6 must stay green;"
    echo "3. drop the \"unreleased snapshot\" caveats in \`docs/dsl.md\` and \`docs/developing.md\`;"
    echo "4. consider whether \`../sdp-example\`'s demo stack should move to the same release."
  fi
} > "$WORK/issue-body.md"

if [[ "$FILE_ISSUE" != true ]]; then
  log "not filing an issue (no --file-issue). Report follows on stdout."
  echo "=== $TITLE ==="
  cat "$WORK/issue-body.md"
  exit 0
fi

command -v gh > /dev/null 2>&1 || fail "--file-issue needs the gh CLI on PATH"
EXISTING="$(gh issue list --label "$ISSUE_LABEL" --state open --limit 1 --json number --jq '.[0].number // empty')"
if [[ -n "$EXISTING" ]]; then
  log "updating issue #$EXISTING"
  gh issue edit "$EXISTING" --title "$TITLE" --body-file "$WORK/issue-body.md" > /dev/null
  echo "updated issue #$EXISTING"
else
  log "opening a new issue"
  # The label must exist; create it idempotently so a fresh repo works.
  gh label create "$ISSUE_LABEL" --description "Automated Spark Connect currency check" --color BFD4F2 > /dev/null 2>&1 || true
  gh issue create --title "$TITLE" --body-file "$WORK/issue-body.md" --label "$ISSUE_LABEL"
fi
