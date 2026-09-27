#!/usr/bin/env bash
# Prints a Markdown report of a run's outcome.json (see run.sh): the
# outcome, the verdict, failures, counters, the throughput results and the
# notes, for the job summary, so that `gh run view` shows how a run went
# without its artifacts.
#
#     integration/ci/summary.sh integration/runs/<id>/outcome.json

set -euo pipefail

jq -r '
  def cell: tostring | gsub("\\|"; "\\|") | gsub("[\r\n]+"; " ");
  def row: map(cell) | "| " + join(" | ") + " |";
  def table($head; $rows):
    ($head | row), ($head | map("---") | row), ($rows[] | row);
  def headline: {
    pass: "PASS",
    fault: "FAIL: SmolNet failed",
    network: "network flakiness, not a SmolNet failure",
    environment: "environment error, not a SmolNet failure"
  }[.outcome];
  def phase_rows: map([
    .family, .phase, .client, .server, .runs, .median_mbit_s,
    "\(.min_mbit_s)-\(.max_mbit_s)", (.ratio_to_kernel // ""), .median_handshake_ms
  ]);

  . as $o | ($o.verdict // {}) as $v |
  "### \($o.id): \($o | headline)",
  "",
  "`\($o.command)`",
  "",
  (if $o.reason != "" then "\($o.reason[:1] | ascii_upcase)\($o.reason[1:]).", "" else empty end),
  "Exit status \($o.exit_status)" +
    (if $o.recheck_status != null
     then "; the kernel recheck exited \($o.recheck_status)" else "" end) + ".",
  "",
  (if $o.verdict == null then "No verdict.json was written.", "" else
    table(["verdict", "mode", "families", "netem", "duration", "elapsed"];
      [[$v.verdict, $v.mode, ($v.families | join(", ")), ($v.netem // "none"),
        "\($v.duration_s) s", "\($v.elapsed_s | . * 10 | round / 10) s"]]),
    "",
    (if $v.error then "**Error:** \($v.error | cell)", "" else empty end),
    (if $v.failure_count > 0 then
      "#### Failures (\($v.failure_count))", "",
      table(["kind", "summary", "file"];
        $v.failures[:20] | map([.kind, .summary, (.file | split("/") | .[-2:] | join("/"))])),
      ""
    else empty end),
    (if ($v.counters | length) > 0 then
      "#### Counters", "",
      table(["counter", "value"]; $v.counters | to_entries | map([.key, .value])),
      ""
    else empty end),
    (if ($v.results.throughput // [] | length) > 0 then
      "#### Throughput", "",
      table(["family", "phase", "client", "server", "runs", "median Mbit/s",
          "min-max", "of kernel", "handshake ms"];
        $v.results.throughput | phase_rows),
      ""
    else empty end),
    ($v.results | del(.throughput) | if length > 0 then
      "<details><summary>Other results</summary>", "", "```json", tojson, "```", "",
      "</details>", ""
    else empty end),
    (if ($v.notes | length) > 0 then
      "<details><summary>Notes (\($v.notes | length))</summary>", "",
      ($v.notes[] | "- " + cell), "", "</details>", ""
    else empty end)
  end)
' "$1"
