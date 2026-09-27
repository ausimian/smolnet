# Integration scenarios

Long-running scripts that drive SmolNet through a real TUN device to the
host's kernel and to the internet. They are opt-in: they need root and
sometimes the internet, so they are kept out of `mix test` and
`mix precommit`. See `SmolNet.Integration.Soak`, under `lib/` here, for
the runner and `SmolNet.Integration.Soak.Options` for the options every
script takes.

## Running one locally

```console
sudo integration/setup.sh              # tun0, addresses and NAT; --no-nat for none
sudo mix run integration/smoke.exs --duration 30s
sudo mix run integration/tls.exs --duration 10m --target local
sudo mix run integration/idle.exs --duration 1h --idle-max 30m
sudo integration/teardown.sh
```

The path MTU scenario, `pmtu.exs`, builds a routed path of its own, with
a hop whose MTU is below SmolNet's, so it runs in a network namespace of
its own, with its own device, which `pmtu-topology.sh isolate` creates.
Where the host allows unprivileged user namespaces, it needs no sudo:

```console
integration/pmtu-topology.sh isolate mix run integration/pmtu.exs
integration/pmtu-topology.sh isolate mix run integration/pmtu.exs --mtu 1280 --family inet
```

It reports each case as `adapts`, `stalls` or `fails`, next to what is
expected of it, and keeps its captures in `captures/`; see
`SmolNet.Integration.Scenarios.Pmtu` and the path MTU guide,
`path_mtu.md`, for what the outcomes mean.

The idle scenario, `idle.exs`, holds up to 240 TCP and TLS connections
for the whole run. Some idle for seconds to hours, some trickle single
bytes, some burst, and some stall on a closed window. A share lose their
peer silently, through nftables (see `SmolNet.Integration.Scenarios.Idle`).
The connections' peers are on the host, so it needs setup.sh's device but
not its NAT. It also runs without sudo in a user namespace of its own,
where a tmpfs on `/run` holds setup.sh's state:

```console
PATH=/usr/sbin:/sbin:$PATH unshare -rnm sh -c 'mount -t tmpfs tmpfs /run &&
  integration/setup.sh --no-nat && mix run integration/idle.exs --duration 30m'
```

The detection times in its notes are measurements, not verdicts. The
first runs, of 200 connections for 30 min with 40 peers vanishing,
found:

| a peer that vanished | SmolNet | Linux (`--baseline`) |
| --- | --- | --- |
| with nothing outstanding (`silent`) | never noticed | never noticed, without keepalive |
| with data unacknowledged (`unacked`, `nat`) | never noticed: it retransmits forever, 60 s apart (#132) | `ETIMEDOUT` after about 940 s |
| while its zero window was being probed (`zero_window`) | never noticed (#132) | not within 970 s |
| and came back as a host that answers with a RST (`reboot`) | 2 to 53 s after the path returned | 1 to 15 s after |

SmolNet does not accept `keepalive` (#133). While all 200 connections
were idle, the stack did not poll once in 120 s, and the VM used 0.24% of
a core.

The chaos scenario, `chaos.exs`, injects faults into a stack while it
carries TLS transfers, connection churn and calls that block for good:
kills of the TUN helper and of the link under each `link_down` policy,
`SmolNet.stop_stack/1`, device and route flaps, egress credit delayed,
trickled or withheld, and socket owners killed or swapped (see
`SmolNet.Integration.Scenarios.Chaos`). Its flaps change the device and
the host's routes, so it runs as `pmtu.exs` does:

```console
integration/pmtu-topology.sh isolate mix run integration/chaos.exs --duration 10m
integration/pmtu-topology.sh isolate mix run integration/chaos.exs --seed 1234 \
  --faults helper_kill --policies mark_down --episodes 3
```

The run logs its seed, and the same `--seed` replays the same faults at
the same times. A failed episode writes `episodes/NN-<fault>-<policy>.txt`,
with its plan, timeline and snapshots, and keeps its capture in
`captures/NN/`; `--keep-going` runs every episode before failing. The
`link_restart` result records, for #43, what a link that could be
replaced would have saved under `:mark_down` and `:notify`.

Each script's `--help` lists its options. `--self-check` runs a scenario
over the TUN helper's loopback with no device and no root, and `--baseline`
over the kernel's stack alone. Use `MIX_ENV=prod` for measurements: the
debug NIF is about 2.5 times slower.

A run exits 0 when it passes, 1 when it fails, and 2 when the environment
is unfit (a target it cannot reach, say) or its arguments are bad. It
writes to `--out` (default `integration/runs/<script>-<time>`):

| file | what |
| --- | --- |
| `verdict.json` | the verdict, `error`, `failures`, `counters`, `notes` and `results` (for TLS, `throughput`) |
| `metrics.csv` | metrics sampled every `--metrics-interval` |
| `run.log` | the run's log |
| `failures/` | on failure, one file per failure with its diagnostics |
| `stack_info.txt` | on failure, `SmolNet.stack_info/1` at the first failure |
| `pcap/` | on failure, the last of the rolling capture on the device |

## On GitHub Actions

`.github/workflows/integration.yml` runs the scenarios on `ubuntu-24.04`
runners, which have passwordless sudo and IPv4 internet access but **no
IPv6 internet**: the TLS scenario notes that it skips IPv6 to the internet,
which is not a failure. IPv6 over the device, to the host, works.

| trigger | runs | files issues |
| --- | --- | --- |
| nightly, 03:17 UTC | 10 min each of `smoke`, `tls --target local` and `tls` to speed.cloudflare.com | yes |
| weekly, Sunday 04:43 UTC | 1 h of `smoke`; 4 h each of `tls --target local`, `tls` to the internet (5 min between rounds) and `idle` (idles of up to 2 h) | yes |
| `workflow_dispatch` | one run from the inputs below | no |
| pull request changing `integration/**` or the workflow | 1 to 2 min each of `smoke`, `smoke` under netem `delay`, `tls --target local` and `tls` to the internet, the whole `pmtu` matrix, and 3 min each of `idle` and `chaos` | no |

`integration/ci/plan.sh` holds the schedule, and `integration/ci/run.sh`
runs `pmtu` and `chaos` under `pmtu-topology.sh isolate`, so that their
routers and flaps never touch the runner's own network. A hosted job may run 6 h,
so the long soak is 4 h rather than 6 h, and a dispatched run at most 5 h.
GitHub disables scheduled workflows after 60 days without a commit to the
repository; re-enable it from the Actions tab.

### Dispatching a run

The branch must contain the workflow, so merge or rebase `main` into it
first.

```console
gh workflow run integration.yml --repo ausimian/smolnet --ref <branch> \
  -f scenario=tls -f duration=10m -f netem=none -f family=inet \
  -f extra_args='--target local --concurrency 8'
```

| input | values | default |
| --- | --- | --- |
| `scenario` | `tls`, `smoke`, `pmtu`, `idle`, `chaos`: runs `integration/<scenario>.exs` | `tls` |
| `duration` | `90s`, `10m`, `1h30m`: at most `5h` | `10m` |
| `netem` | `none` or a profile of `SmolNet.Integration.Soak.Netem` | `none` |
| `family` | `both`, `inet`, `inet6` | `both` |
| `extra_args` | more options, as plain words, such as `--baseline` or `--round-pause 0` | none |

`extra_args` may hold only letters, digits, spaces and `. _ : = / + , -`,
and not the options the workflow sets itself (`--duration`, `--family`,
`--netem`, `--out`, `--device`). Without `--target local`, the TLS scenario
goes to speed.cloudflare.com; keep internet runs short and at the default
rates. The run is built with the release NIF.

### Reading the results

```console
gh run list --repo ausimian/smolnet --workflow integration.yml --limit 5
gh run watch <run-id> --repo ausimian/smolnet
gh run view <run-id> --repo ausimian/smolnet             # each run's annotation
gh run view <run-id> --repo ausimian/smolnet --log        # console output
gh run download <run-id> --repo ausimian/smolnet --name results-<id>
```

Each run's job, named by its id (`dispatch-<scenario>` for a dispatched
one), leaves one annotation, which `gh run view` prints: the outcome and
why, the verdict, the counters, the first failures and the throughput of
each phase, next to the kernel's. Its job summary, in the browser, adds
tables and the notes. The `results-<id>` artifact (kept 30 days) holds `<id>/` with
the files above, plus `console.log` and `outcome.json`, and
`<id>-recheck/` if the path was rechecked. A failed run's capture is the
`pcap-<id>` artifact (kept 7 days).

`outcome.json` is what the workflow decided, with the run, the exit status
and `verdict.json` under `verdict`:

```console
jq '{outcome, reason, verdict: .verdict.verdict, throughput: .verdict.results.throughput}' \
  <id>/outcome.json
```

### Outcomes, and what is filed

`integration/ci/run.sh` separates SmolNet's failures from the network's:

| outcome | when | job | filed |
| --- | --- | --- | --- |
| `pass` | exit 0 | passes | no |
| `environment` | exit 2: the host was unfit or the arguments bad | passes, with a warning | no |
| `network` | the run failed, but it was the kernel baseline (`--baseline`), or every failure was a kernel transfer or DNS; or it reaches the internet and one round of the kernel baseline to the same target, run straight after, failed too | passes, with a warning | no |
| `fault` | any other failure, a crash, or a run past its time | fails | if scheduled |

The TLS scenario already checks that the host reaches its target before it
starts, and abandons the run (exit 2) if SmolNet cannot while the host
can. Only scheduled runs file issues: the report job comments on the open
issue labelled `integration:<scenario>` or, if there is none, opens one,
creating the label if it is missing. Close the issue once its failures
are fixed; the next one opens a new issue.
