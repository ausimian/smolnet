# ADR 0017: the smoltcp NIF stays; the Elixir engine is abandoned

## Status

Accepted. Supersedes ADR 0016's go decision. Closes #173 as not planned.

## Context

ADR 0016 recorded a go for #173, replacing the smoltcp NIF with an
Elixir protocol engine, on the strength of a throwaway prototype that
ran at 1.01–1.42× the NIF unimpaired. #173's decision 1 set the floor:
at least 0.5× the NIF's unimpaired single-stream rate, and within 10%
of the NIF on every impaired profile.

Phases 1 to 6 of #173 then built the real engine on the feature branch
`pure-elixir-stack`, through PRs #175, #176, #178, #179, #180, #183,
#186, #187, #188 and #190:

- A backend seam in `SmolNet.Stack`, with the NIF as one backend.
- Smoltcp's 238 TCP socket tests dumped as replayable vectors, NIF
  traces, a lossy-link simulator and a differential harness.
- A pure Elixir engine: wire formats, the IP layer, the socket table and
  UDP, and TCP with NewReno, SACK recovery, RACK-TLP, D-SACK and CUBIC.
  It replayed all 230 vectors field for field, and matched the NIF byte
  for byte in 100,000 random impaired transfers in every pairing of the
  two engines.

Phase 6 measured the engine on the netem matrix and found it far below
the floor. A performance pass followed (#191, merged into the branch,
and #192, which was not merged).

## Measurements

All on a 20-core Debian host, `MIX_ENV=prod`, both engines in one
session, `--egress-credit infinity`.

### Netem matrix, `none` profile

Ratio of the Elixir engine to the NIF, from the medians of two sessions
of 8 MiB transfers on cores 16–19:

| family | flow | 1 stream | 4 streams |
| --- | --- | --- | --- |
| inet | send | 0.37 | 0.31–0.34 |
| inet | receive | 0.37 | 0.32–0.34 |
| inet6 | send | 0.37–0.39 | 0.25–0.29 |
| inet6 | receive | 0.34–0.35 | 0.32 |

This is with #192's optimisations. Without them, the engine on the branch
tip ran at 0.14–0.28× single-stream (phase 6, #190).

With a fresh stack for each transfer, so that no earlier connection lingers
in TIME-WAIT, the single-stream ratios are 0.38–0.44 sending and
0.45–0.47 receiving for inet, and 0.34–0.40 sending and 0.45–0.46
receiving for inet6.

Profiles bound by delay (`delay`, `high-bdp`, `bufferbloat`, one-stream
`reorder` and `jitter`) were at parity in phase 6. The profiles bound by
CPU (`corrupt`, `duplicate`, `loss-burst`, four-stream `reorder`) were
0.10–0.41× there, and were not measured again after #192.

### Cost per MiB

- **Engine alone** (`bench/core.exs`, both ends of one transfer in one
  process, cores 8–11): 13.4–13.8 ms/MiB, against 4.0 for the NIF in
  the same harness. It was 23.2 at phase 6, 19.5 after the
  garbage-collection settings of ADR 0016 (#191), and 13.4–13.8 after
  #192.
- **Loopback** (one stack carrying a transfer to itself through the
  socket API): 0.43–0.49× the NIF's rate, at 26–28 ms of CPU per MiB
  against 13. Garbage collection was 0.8 ms/MiB, about the NIF's, once
  ADR 0016's settings were applied.

### Where the engine's time goes

A traced profile of the optimised engine, scaled to its untraced
13.6 ms/MiB, splits it this way:

| category | ms/MiB | share |
| --- | --- | --- |
| drive, demultiplexing and wakes | 2.8 | 21% |
| send decision (dispatch, deadlines) | 2.6 | 19% |
| segment input (checks, ACK, payload) | 2.3 | 17% |
| generic map, list and struct work | 1.5 | 11% |
| parse and emit | 1.4 | 10% |
| RACK, scoreboard, CUBIC, RTT and timers | 0.8 | 6% |
| segment build | 0.8 | 6% |
| core call, effects and counters | 0.7 | 5% |
| buffers | 0.5 | 4% |
| checksums | 0.1 | 1% |

The profile attributes little to checksums, but a microbenchmark puts a
1,480-byte checksum at about 480 ns, roughly 0.7 ms/MiB for both ends.
Either way the cost is spread out, with no hot spot left. The engine
does about 840,000 function calls per MiB, both ends together.

The prototype in ADR 0016 cost less mainly because it did less: it had
no RACK send log, SACK scoreboard, CUBIC, wakes or socket table, and no
TIME-WAIT connections to walk.

## Decision

**No-go.** The NIF stays the only protocol engine. The Elixir engine is
abandoned, and the `pure-elixir-stack` branch is deleted.

- Single-stream unimpaired throughput is 0.34–0.39× the NIF on the netem
  matrix, and at best 0.47× on a fresh stack, against a floor of 0.5×.
- The rest of the gap has no single cause. The performance pass
  estimated, from an analysis it did not finish, that a further 3 to 5
  weeks of restructuring might reach about 0.5× (±0.07) single-stream,
  with a 35–40% chance of clearing the floor on all four single-stream
  rows. Coming within 10% of the NIF on the profiles bound by CPU was
  judged out of reach for pure Elixir.
- A checksum NIF would recover about 0.03× and would bring back a
  native dependency, which is what the work set out to remove.

ADR 0016 is superseded. Its prototype's numbers stand, but they did not
predict the real engine's cost.

## Consequences

- The NIF, the vendored smoltcp fork and the precompiled artifacts stay,
  as ADRs 0002, 0010, 0011, 0013 and 0015 describe. No release changed.
- The open TCP issues that #173 deferred to its phase 10 go back to the
  NIF: #117, #124, #125, #139, #142, #151, #154, #155, #158, #159, #160,
  #161 and #169.
- The port found NIF bugs that remain open: #177 (a FIN lost after the
  data is never resent) and #189 (a late zero-window ACK silences the
  sender until the user timeout). It also filed smoltcp quirks the port
  reproduced on purpose: #181, #182 and #184. #185 assumed a cutover that
  will not happen.
- The conformance harnesses, the NIF traces and the backend seam went
  with the branch. #192's head, `0542db4`, holds the last complete
  version of the work, and GitHub keeps it as `refs/pull/192/head`.
  Bringing the test tooling back to `main` would be separate work.
