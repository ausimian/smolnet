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
sudo integration/teardown.sh
```

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
| weekly, Sunday 04:43 UTC | 1 h of `smoke`; 4 h each of `tls --target local` and `tls` to the internet, 5 min between rounds | yes |
| `workflow_dispatch` | one run from the inputs below | no |
| pull request changing `integration/**` or the workflow | 1 to 2 min each of `smoke`, `smoke` under netem `delay`, `tls --target local` and `tls` to the internet | no |

`integration/ci/plan.sh` holds the schedule. A hosted job may run 6 h,
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
| `scenario` | `tls`, `smoke`: runs `integration/<scenario>.exs` | `tls` |
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
