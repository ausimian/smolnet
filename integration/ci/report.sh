#!/usr/bin/env bash
# Files a scheduled run's SmolNet failures: for each scenario with a run
# whose outcome is `fault` (see run.sh), comments on the open issue
# labelled integration:<scenario>, or opens one with that label, creating
# the label if it is missing. Other outcomes are only reported: network
# flakiness and environment errors are never filed, and a planned run that
# left no outcome (cancelled, or broken before it ran) gets a warning.
#
#     integration/ci/report.sh OUTCOMES_DIR
#
# Reads MATRIX (plan.sh's), GITHUB_REPOSITORY, GITHUB_RUN_ID and
# GITHUB_SERVER_URL, and GH_TOKEN for gh, which needs issues: write.

set -euo pipefail

dir=$1
repo=$GITHUB_REPOSITORY
run_url="${GITHUB_SERVER_URL:-https://github.com}/$repo/actions/runs/$GITHUB_RUN_ID"

outcomes=$(find "$dir" -name outcome.json -print0 | xargs -0 -r cat | jq -cs 'sort_by(.id)')

# Planned runs with no outcome.
while read -r id; do
  echo "::warning title=$id left no outcome::the run of $id was cancelled or broke before it ran; see $run_url"
done < <(jq -rn --argjson planned "$MATRIX" --argjson finished "$outcomes" \
  '($finished | map(.id)) as $ids | $planned.include[].id | select(. as $id | $ids | index($id) | not)')

jq -r '.[] | "\(.id): \(.outcome)"' <<<"$outcomes"

# ensure_label NAME SCENARIO
ensure_label() {
  local labels
  labels=$(gh label list --repo "$repo" --limit 1000 --json name --jq '.[].name')

  if ! grep -Fxq "$1" <<<"$labels"; then
    gh label create "$1" --repo "$repo" --color d93f0b \
      --description "SmolNet failures found by scheduled $2 integration runs"
  fi
}

# body SCENARIO: the Markdown for this run's failures of SCENARIO.
body() {
  jq -r --arg scenario "$1" --arg run_url "$run_url" --arg repo "$repo" \
    --arg run_id "$GITHUB_RUN_ID" '
    def cell: tostring | gsub("\\|"; "\\|") | gsub("[\r\n]+"; " ");
    map(select(.scenario == $scenario and .outcome == "fault")) |
    "The scheduled integration run \($run_url) found SmolNet failures in the `\($scenario)` scenario.",
    "",
    (.[] |
      "#### \(.id)",
      "",
      "`\(.command)`",
      "",
      "\(.reason) (exit status \(.exit_status)).",
      "",
      (if .verdict != null and .verdict.failure_count > 0 then
        "| kind | summary |", "| --- | --- |",
        (.verdict.failures[:10][] | "| \(.kind | cell) | \(.summary | cell) |"),
        ""
      else empty end),
      "Artifacts: `gh run download \($run_id) --repo \($repo) --name results-\(.id)`" +
        " (and `--name pcap-\(.id)` for the capture).",
      "")
  ' <<<"$outcomes"
}

mapfile -t scenarios < <(jq -r 'map(select(.outcome == "fault") | .scenario) | unique | .[]' <<<"$outcomes")

for scenario in "${scenarios[@]}"; do
  label="integration:$scenario"
  ensure_label "$label" "$scenario"
  file=$(mktemp)
  body "$scenario" >"$file"

  number=$(gh issue list --repo "$repo" --label "$label" --state open --json number \
    --jq 'map(.number) | min // empty')

  if [[ -n $number ]]; then
    gh issue comment "$number" --repo "$repo" --body-file "$file"
  else
    {
      echo
      echo "Scheduled runs of \`integration/$scenario.exs\` (see integration/README.md) add later"
      echo "failures to this issue as comments while it is open, so close it once they are fixed."
      echo "Part of #85."
    } >>"$file"
    gh issue create --repo "$repo" --label "$label" --body-file "$file" \
      --title "Scheduled integration runs of $scenario find SmolNet failures"
  fi

  rm -f "$file"
done
