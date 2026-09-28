#!/usr/bin/env bash
# Plans the runs of .github/workflows/integration.yml: writes the job
# matrix for the event that started the workflow to $GITHUB_OUTPUT (or
# stdout), as `matrix={"include": [RUN, ...]}`. Each RUN is
#
#     {"id": "tls-internet", "scenario": "tls", "duration": "10m",
#      "netem": "", "family": "both", "args": ["--round-pause", "300000"],
#      "internet": true, "backstop_s": 1560, "timeout": 42}
#
# where `args` are passed to integration/<scenario>.exs after the workflow's
# own --duration, --family, --netem and --out, `internet` says whether the
# run reaches the internet (so a failure is rechecked over the kernel's
# stack before it counts; see run.sh), `backstop_s` bounds the script's
# wall-clock time and `timeout` the job's, in minutes.
#
# Reads EVENT_NAME, SCHEDULE (github.event.schedule) and, for
# workflow_dispatch, INPUT_SCENARIO, INPUT_DURATION, INPUT_NETEM,
# INPUT_FAMILY and INPUT_EXTRA_ARGS. The inputs are validated here and
# only ever passed on as separate arguments, never through a shell.

set -euo pipefail

# Keep these in step with the `schedule` crons in integration.yml.
NIGHTLY='17 3 * * *'
WEEKLY='43 4 * * 0'

# A hosted runner's job may run 6 h; this leaves room for the build, the
# harness's overrun grace (a tenth of the duration) and the upload.
MAX_DURATION_MS=$((5 * 3600 * 1000))
MAX_TIMEOUT_MIN=360

cd "$(dirname "$0")/../.."

die() {
  echo "::error title=integration plan::$*" >&2
  exit 1
}

# Prints TEXT in milliseconds, parsed as
# SmolNet.Integration.Soak.Options.parse_duration/1 does: whole seconds, or
# amounts with units ms, s, m, h or d, such as 90s or 1h30m.
duration_ms() {
  local text=$1 total=0 amount

  if [[ $text =~ ^[0-9]{1,9}$ ]]; then
    echo $((10#$text * 1000))
    return
  fi

  [[ $text =~ ^([0-9]{1,9}(ms|s|m|h|d))+$ ]] || return 1

  while [[ -n $text ]]; do
    [[ $text =~ ^([0-9]+)(ms|s|m|h|d)(.*)$ ]] || return 1
    amount=$((10#${BASH_REMATCH[1]}))
    text=${BASH_REMATCH[3]}

    case ${BASH_REMATCH[2]} in
      ms) ;;
      s) amount=$((amount * 1000)) ;;
      m) amount=$((amount * 60000)) ;;
      h) amount=$((amount * 3600000)) ;;
      d) amount=$((amount * 86400000)) ;;
    esac

    total=$((total + amount))
  done

  echo "$total"
}

# Whether ARGS (a JSON array) point the TLS or crawl scenario at a local
# target or the helper's loopback, so that it does not reach the internet.
offline() {
  jq -e 'index("--self-check") or index("--target=local")
    or (index("--target") as $i | $i != null and .[$i + 1] == "local")' <<<"$1" >/dev/null
}

# run ID SCENARIO DURATION NETEM FAMILY [ARG...]: prints one RUN.
run() {
  local id=$1 scenario=$2 duration=$3 netem=$4 family=$5
  shift 5

  [[ -f integration/$scenario.exs ]] || die "no scenario integration/$scenario.exs"

  local ms
  ms=$(duration_ms "$duration") || die "invalid duration '$duration': use 90s, 10m, 4h or 1h30m"
  ((ms <= MAX_DURATION_MS)) || die "duration '$duration' is over the 5h a hosted runner allows"

  local args internet=false
  args=$(jq -cn '$ARGS.positional' --args -- "$@")
  if [[ $scenario == tls || $scenario == crawl ]] && ! offline "$args"; then internet=true; fi

  # The harness gives a workload a tenth of its duration, at least a
  # minute, to finish; the backstop adds 5 minutes to that for set-up and
  # the socket-count check, and the job 30 more for everything else.
  local grace_s=$((ms / 10000 > 60 ? ms / 10000 : 60))
  local backstop_s=$(((ms + 999) / 1000 + grace_s + 300))
  local timeout=$(((backstop_s + 59) / 60 + 30))
  ((timeout <= MAX_TIMEOUT_MIN)) || timeout=$MAX_TIMEOUT_MIN

  jq -cn \
    --arg id "$id" --arg scenario "$scenario" --arg duration "$duration" \
    --arg netem "$netem" --arg family "$family" --argjson args "$args" \
    --argjson internet "$internet" --argjson backstop_s "$backstop_s" \
    --argjson timeout "$timeout" \
    '{id: $id, scenario: $scenario, duration: $duration, netem: $netem,
      family: $family, args: $args, internet: $internet,
      backstop_s: $backstop_s, timeout: $timeout}'
}

# The one run a workflow_dispatch asks for.
dispatch() {
  local scenario=${INPUT_SCENARIO:-} duration=${INPUT_DURATION:-} netem=${INPUT_NETEM:-none}
  local family=${INPUT_FAMILY:-both} extra=${INPUT_EXTRA_ARGS:-}

  [[ $scenario =~ ^[a-z][a-z0-9_]*$ ]] || die "invalid scenario '$scenario'"
  [[ $family =~ ^(both|inet|inet6)$ ]] || die "family must be both, inet or inet6, got '$family'"
  [[ $netem =~ ^[a-z0-9-]+$ ]] || die "invalid netem profile '$netem'"
  [[ $netem == none ]] && netem=""

  # Plain words only: no quoting, globbing or expansion to get wrong.
  [[ $extra =~ ^[A-Za-z0-9\ ._:=/+,-]*$ ]] ||
    die "extra_args may hold only letters, digits, spaces and . _ : = / + , -"

  local words=() word
  read -r -a words <<<"$extra"

  for word in "${words[@]}"; do
    case $word in
      --duration | --duration=* | --family | --family=* | --netem | --netem=*)
        die "pass ${word%%=*} as its own input, not in extra_args"
        ;;
      --out | --out=* | --device | --device=* | --help)
        die "the workflow sets ${word%%=*} itself"
        ;;
    esac
  done

  run "dispatch-$scenario" "$scenario" "$duration" "$netem" "$family" "${words[@]}"
}

# Prints the RUNs for this event, one per line.
plan() {
  case ${EVENT_NAME:-} in
    schedule) scheduled ;;
    workflow_dispatch) dispatch ;;
    # A pull request that changes the harness or this workflow gets a
    # short run of each kind, netem included, to prove the plumbing.
    pull_request)
      run smoke smoke 1m '' both
      run smoke-netem smoke 1m delay both
      run tls-local tls 2m '' both --target local --round-pause 10000
      run tls-internet tls 1m '' both
      # The whole matrix, which ends on its own in a few minutes.
      run pmtu pmtu 15m '' both
      # Long enough for a reboot's path to return and be detected, and,
      # with the timers shortened, for every other vanished peer to be
      # detected too. The first keep-alive probes come after the quiet
      # window. Its bursts use memory for most of the run, which is all
      # warm-up.
      run idle idle 4m '' both --quiet 30s --idle-max 1m --trickle-max 10s \
        --outage-max 20s --nat-timeout 20s --warmup 2m --keepalive \
        --user-timeout 45s --keepalive-idle 50s --keepalive-interval 2s \
        --keepalive-probes 3
      # A cycle of every fault and policy takes about a minute.
      run chaos chaos 3m '' both --keep-going
      # Local servers only, never the internet: a round of each outcome,
      # and the socket ceiling pushed past, over both families.
      run crawl-local crawl 2m '' both --target local --ceiling-step 5000 --round-pause 10000 \
        --max-smolnet-only 0
      # One pass of the matrix, small transfers, every profile but
      # loss-burst, whose one-stream sends can wait minutes on SmolNet's
      # backed-off retransmission timer (#138).
      run netem-matrix netem_matrix 10m '' both --no-pcap --repeats 1 --bytes 2097152 \
        --profiles none,bufferbloat,corrupt,delay,duplicate,high-bdp,reorder
      ;;
    *) die "no runs are planned for the event '${EVENT_NAME:-}'" ;;
  esac
}

scheduled() {
  case ${SCHEDULE:-} in
    "$NIGHTLY")
      run smoke smoke 10m '' both
      run tls-local tls 10m '' both --target local
      run tls-internet tls 10m '' both
      ;;
    # The long soaks: at most 5 h fits a hosted runner's 6 h job, so the
    # 6 h soak of #85 runs as these 4 h ones, one runner each. The
    # internet one pauses 5 minutes between rounds to keep its load on
    # speed.cloudflare.com polite.
    "$WEEKLY")
      run smoke-1h smoke 1h '' both
      run tls-local-4h tls 4h '' both --target local
      run tls-internet-4h tls 4h '' both --round-pause 300000
      # Idles of seconds to hours, and NAT timeouts of half an hour, with
      # SmolNet's own timers: every vanished peer is detected, silent ones
      # by keepalive after 2 h.
      run idle-4h idle 4h '' both --quiet 5m --idle-max 2h --outage-max 10m \
        --nat-timeout 30m --keepalive
      # The Tranco top 1,000, weekly rather than nightly so that each site
      # sees a dozen or so HEADs a week: about 8 rounds, 5 minutes apart,
      # 16 in flight. Only a host SmolNet persistently fails to reach while
      # the kernel reaches it is a fault; one that fails over both never is.
      run crawl-1h crawl 1h '' both --max-smolnet-only 0
      ;;
    *) die "no runs are planned for the schedule '${SCHEDULE:-}'" ;;
  esac
}

# Not in a command substitution, so that a `die` stops the script.
runs=$(mktemp)
trap 'rm -f "$runs"' EXIT
plan >"$runs"

matrix=$(jq -cs '{include: .}' "$runs")
echo "matrix=$matrix" >>"${GITHUB_OUTPUT:-/dev/stdout}"
