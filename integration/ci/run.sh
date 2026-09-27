#!/usr/bin/env bash
# Runs one RUN that plan.sh planned, given as JSON in $RUN, as root with
# the release NIF (MIX_ENV=prod), and judges it. The device must be up
# (integration/setup.sh) and the project compiled with MIX_ENV=prod.
#
# Everything goes to $RUNS_DIR/<id> (default integration/runs/<id>): the
# harness's artifacts, console.log (the script's output), and outcome.json,
# which holds the RUN, the exit status, verdict.json and one outcome:
#
#   pass         the script passed (exit 0).
#   environment  the script found the host unfit, or its arguments bad
#                (exit 2, SmolNet.Integration.Soak.abandon/2): never a
#                SmolNet failure.
#   network      the script failed, but the path is to blame: the run was
#                the kernel baseline, every failure was the kernel's own
#                transfer or a DNS lookup, or, for a run that reaches the
#                internet, one round of the kernel baseline to the same
#                target, run straight after into <id>-recheck, failed too.
#   fault        anything else: SmolNet failed.
#
# It appends a report to $GITHUB_STEP_SUMMARY and annotates the run. It
# exits 0 whatever the outcome; the workflow fails a `fault` afterwards,
# once the artifacts are uploaded.

set -euo pipefail

cd "$(dirname "$0")/../.."

field() { jq -r --arg name "$1" '.[$name]' <<<"$RUN"; }

id=$(field id)
scenario=$(field scenario)
family=$(field family)
netem=$(field netem)
internet=$(field internet)
backstop_s=$(field backstop_s)
mapfile -t extra < <(jq -r '.args[]' <<<"$RUN")

runs_dir=${RUNS_DIR:-integration/runs}
out=$runs_dir/$id
recheck=$runs_dir/$id-recheck
mkdir -p "$out"

argv=("integration/$scenario.exs" --duration "$(field duration)" --family "$family")
if [[ -n $netem ]]; then argv+=(--netem "$netem"); fi
argv+=("${extra[@]}" --out "$out")
command="mix run ${argv[*]}"

# soak SECONDS ARG...: runs `mix run ARG...` as root, as ci.yml's smoke job
# does, killed if it outlasts SECONDS.
soak() {
  local seconds=$1
  shift
  sudo -E env "PATH=$PATH" "HOME=$HOME" MIX_ENV=prod \
    timeout --kill-after=60 "$seconds" mix run "$@"
}

echo "$command"
set +e
soak "$backstop_s" "${argv[@]}" 2>&1 | tee "$out/console.log"
status=${PIPESTATUS[0]}
set -e

verdict=$out/verdict.json
if ! jq -e . "$verdict" >/dev/null 2>&1; then verdict=""; fi

# Whether every failure in the verdict is the kernel's own or a DNS
# lookup's, which only the path can explain. A kernel transfer's summary
# names its client ("kernel client to"), and its deadline's the tuple
# {phase, family, "kernel", server, index}.
path_failures_only() {
  jq -e '.failures | length > 0 and all(
    .kind == "dns" or (.summary | test("kernel client to |, \"kernel\", \"")))' \
    "$verdict" >/dev/null
}

recheck_status=null

# One round of the kernel baseline to the internet target, with the run's
# families. It passing means the path worked, so the failure was SmolNet's.
recheck() {
  mkdir -p "$recheck"
  echo "rechecking the path over the kernel's stack"
  set +e
  soak 900 integration/tls.exs --baseline --duration 1s --family "$family" --out "$recheck" 2>&1 |
    tee "$recheck/console.log"
  recheck_status=${PIPESTATUS[0]}
  set -e
}

if [[ $status == 0 ]]; then
  outcome=pass
  reason=""
elif [[ $status == 2 ]]; then
  outcome=environment
  reason="the host was unfit or the arguments bad: "
  if [[ -n $verdict ]]; then
    reason+=$(jq -r '.error // "no error recorded"' "$verdict")
  else
    reason+=$(grep -v '^[[:space:]]*$' "$out/console.log" | head -n 1 || true)
  fi
elif [[ $status == 124 || $status == 137 ]]; then
  # timeout's own statuses: the harness's overrun grace did not end it.
  outcome=fault
  reason="the script outlasted its ${backstop_s} s backstop and was stopped"
elif [[ -z $verdict ]]; then
  outcome=fault
  reason="the script exited $status without a verdict; see console.log"
elif [[ $(jq -r .mode "$verdict") == kernel ]]; then
  outcome=network
  reason="the kernel baseline failed, so the path is to blame, not SmolNet"
elif [[ $internet == true ]] && path_failures_only; then
  outcome=network
  reason="only the kernel's own transfers or DNS failed, so the path is to blame"
elif [[ $internet == true ]]; then
  recheck
  if [[ $recheck_status == 0 ]]; then
    outcome=fault
    reason="SmolNet failed, and the kernel then passed a round on the same path"
  else
    outcome=network
    reason="SmolNet failed, but the kernel then failed a round on the same path too (exit $recheck_status)"
  fi
else
  outcome=fault
  reason="SmolNet failed; see the failures"
fi

run_url="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-}/actions/runs/${GITHUB_RUN_ID:-}"

jq -n \
  --argjson run "$RUN" --arg command "$command" --arg outcome "$outcome" \
  --arg reason "$reason" --argjson status "$status" \
  --argjson recheck_status "$recheck_status" --arg run_url "$run_url" \
  --slurpfile verdict "${verdict:-/dev/null}" \
  '$run + {command: $command, outcome: $outcome, reason: $reason,
    exit_status: $status, recheck_status: $recheck_status, run_url: $run_url,
    verdict: ($verdict[0] // null)}' >"$out/outcome.json"

integration/ci/summary.sh "$out/outcome.json" >>"${GITHUB_STEP_SUMMARY:-/dev/stdout}"

# An annotation's message is one line, with % and line breaks escaped.
message=${reason//'%'/'%25'}
message=${message//$'\r'/'%0D'}
message=${message//$'\n'/'%0A'}

case $outcome in
  pass) echo "$id passed" ;;
  fault) echo "::error title=$id failed::$message" ;;
  network) echo "::warning title=$id: network flakiness, not filed::$message" ;;
  environment) echo "::warning title=$id: environment error, not filed::$message" ;;
esac
