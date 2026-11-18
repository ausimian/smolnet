# ADR 0016: an Elixir protocol engine is fast enough on the unimpaired path

## Status

Proposed. Records phase 0 of #173. The throughput floor below is the
plan's recommendation (decision 1 of #173), which has not been confirmed.

## Context

#173 plans to replace the vendored smoltcp NIF with an Elixir engine
behind the same public API. One unknown decided whether to start:
throughput. #173's decision 1 recommends a go/no-go floor. The Elixir
engine must reach at least half the NIF's unimpaired single-stream rate,
and come within 10% of the NIF on every impaired profile. Phase 0 is a
spike that measures this against the NIF in the same harness each time,
because the floor is a ratio.

## The prototype

The throwaway branch `spike/phase-0-throughput` (at c938834, not for
merging) holds an engine of about 2,000 lines, `SmolNet.Spike.Engine`.
It implements the `SmolNet.Native` functions that a `:gen_tcp` bulk
transfer calls, so `SmolNet.Stack`, the socket API, the `:gen_tcp`
adapter and the TUN link run in front of it unchanged. Before writing it,
every NIF call and result over a transfer was recorded, so the engine
copies the NIF's call surface and its `recv`, `send`, `accept` and
readiness semantics rather than inferring them.

It has:

- IPv4 and TCP parse and emit, with checksums summed eight 32-bit words
  a step.
- A handshake that negotiates MSS, window scale, SACK-permitted and
  timestamps.
- Send and receive queues of binaries, sliced and never copied byte by
  byte.
- A 32-range out-of-order assembler.
- Cumulative and SACK ACK processing.
- RFC 6298 RTO with smoltcp's 200 ms floor, and a 10 ms delayed ACK
  (every second segment at once).
- Slow start and congestion avoidance with byte counting, fast
  retransmit with NewReno partial ACKs, Minshall's Nagle, and egress
  credit.

It runs in one process. The engine is a value in the Stack process's
dictionary, keyed by the reference the Stack takes for the NIF resource,
and it sends readiness messages itself, as the NIF does.

It lacks CUBIC, RACK-TLP, RFC 6675 recovery, D-SACK, keepalive and the
user timeout, path MTU, IPv6, UDP, ICMP, close deadlines, and most error
paths. On an unimpaired path these add per-ACK bookkeeping rather than
work per byte, but they are not free. So the prototype's figures are an
upper bound for a full engine.

## Method

Both harnesses used the release NIF (`MIX_ENV=prod`; the build logs
"Compiling crate smolnet_nif in release mode") and OTP 28 with Elixir
1.19.5. Every transfer was random data checked by length and SHA-256 at
the receiver.

- **Harness 1, engine cost.** Two stacks in one VM, joined by a bare
  relay process each way (one serialized feeder per stack), carry
  256 MiB transfers over `:gen_tcp`, split over 1 or 4 connections. The
  four pairings, NIF↔NIF, spike↔spike, spike→NIF and NIF→spike, ran
  interleaved over 5 repeats on macOS on an Apple M4 Pro, with 12
  schedulers. A second pass profiled 128 MiB transfers with scheduler
  busy-waiting off (`+sbwt none`). It recorded the VM's CPU time and
  traced each Stack process's time on CPU and its garbage collections.
- **Harness 2, the real path.** `integration/netem_matrix.exs` with
  `--profiles none --family inet --streams 1,4 --bytes 268435456` ran
  under `netem-topology.sh isolate`, in a privileged Linux container
  (kernel 7.0.14-linuxkit, Docker Desktop on the same host), pinned with
  `taskset` to 4 cores as `integration/netem.md`'s runs are.
  `SMOLNET_ENGINE` chose the engine, and each run logged the engine its
  stacks started on. The NIF, the spike, and the spike with GC tuning
  (below) ran interleaved over 3 rounds of 3 repeats: 9 transfers a case.
  GNU `time` measured each whole run's CPU. That kernel has no `ifb`, so
  no impaired profile could run.

The raw results are in `0016-results/`. Absolute rates are this host's,
and are about three times `netem.md`'s, which were taken on a 20-core NUC.
Only the ratios carry over.

## Results

### Harness 2: through the TUN helper to a Linux kernel peer

Median Mbit/s over 9 transfers (min to max), and the ratio to the NIF.
Every run passed: every transfer was intact, and none stalled.

| flow | streams | NIF | spike | spike, GC tuned |
| --- | --- | --- | --- | --- |
| SmolNet sending | 1 | 2,648 (2,383–2,954) | 2,914 (2,879–3,081), **1.10×** | 3,464 (3,393–3,567), **1.31×** |
| SmolNet sending | 4 | 2,934 (2,563–3,254) | 2,970 (2,852–3,172), **1.01×** | 3,715 (3,414–3,940), **1.27×** |
| SmolNet receiving | 1 | 1,198 (1,168–1,425) | 1,435 (1,419–1,484), **1.20×** | 1,571 (1,529–1,660), **1.31×** |
| SmolNet receiving | 4 | 1,212 (1,158–1,334) | 1,570 (1,535–1,632), **1.30×** | 1,719 (1,694–1,755), **1.42×** |

The kernel sending to the kernel across the veth ran at 6,084 Mbit/s with
one stream and 18,674 with four. The whole run's CPU (user and system,
every process, the kernel flows included) was 35.2 s for the NIF, 31.9 s
for the spike and 29.5 s for the spike with GC tuning, the medians of 3
runs.

### Harness 1: two stacks in one VM

Median Mbit/s over 5 transfers of 256 MiB (min to max):

| pairing | 1 stream | 4 streams |
| --- | --- | --- |
| NIF → NIF | 2,431 (1,826–2,554) | 1,781 (1,530–1,825) |
| spike → spike | 2,682 (2,244–2,714) | 2,210 (1,966–2,286) |
| spike → NIF | 2,440 (1,839–2,515) | 1,740 (1,524–1,784) |
| NIF → spike | 2,682 (2,216–2,715) | 2,371 (2,060–2,402) |

The spike over the NIF is 1.10× with one stream and 1.24× with four. The
mixed pairings interoperate both ways, and the pairings with the spike
receiving are the fastest.

The profile, per MiB transferred, for 128 MiB transfers on one stream:

| | NIF | spike | spike, GC tuned |
| --- | --- | --- | --- |
| VM CPU | 10.8 ms | 10.0 ms | 9.1 ms |
| sending Stack process, on CPU | 3.1 ms | 3.3 ms | 2.2 ms |
| receiving Stack process, on CPU | 4.7 ms | 3.9 ms | 2.9 ms |
| sending Stack, GCs in the transfer (major) | 40,672 (169) | 28,689 (9,562) | 1,775 (295) |
| sending Stack, time in GC | 43 ms | 111 ms | 14 ms |

The ratios with four streams match. Neither Stack process was saturated:
the NIF's receiving Stack was the busiest, at 81% on CPU. The ceiling in
this harness is shared among the Stack, the adapter and the relay
processes.

### Garbage collection

Untuned, the spike's sending Stack runs a major collection every
few hundred KiB. Its socket buffers are reference-counted binaries
referenced from its heap, and they fill the default binary virtual heap.
Setting `min_bin_vheap_size` to 1 Mi words and `min_heap_size` to
32 Ki words on the Stack process, from inside the engine, cut its time in
GC by 8×. It also cut its CPU per MiB by about a third, and lifted
harness 2 from 1.01–1.30× of the NIF to 1.27–1.42×. Process heaps stayed
under 150 KB in every case; the buffered data is off-heap. The longest
single pause was not measured.

## Decision

**Go**, on the unimpaired half of the floor.

- The floor asks for at least 0.5× the NIF's unimpaired single-stream
  rate. The prototype reached 1.10× sending and 1.20× receiving in
  harness 2, and 1.31× each way with GC tuning. With four streams it
  reached 1.01× to 1.42×.
- The headroom covers what the prototype leaves out. Per MiB, the
  spike's Stack processes used no more CPU than the NIF's, and with GC
  tuning about two thirds of it. Where the Stack process limits the rate,
  a full engine could cost about twice the prototype's CPU per packet and
  still meet the floor.
- **The impaired half of the floor is not evaluated.** It needs the
  loss recovery and congestion control of phases 5 and 6. On profiles
  bound by delay, the rate depends on the engine's behaviour rather than
  its CPU, and the prototype has neither CUBIC nor RACK-TLP. The test
  host's kernel also lacks `ifb`, which every impaired profile needs.
  Phase 8 judges it, against the netem matrix, as #173 already plans.

Consequences for the plan:

- **Process model (decision 2):** one process per stack is enough. No
  Stack process was saturated, and nothing here argues for a process per
  socket.
- **Envelope:** the spike never used `more`. A full ingress, send or
  receive completed within its call in this workload. Whether long
  ingress batches starve callers is still to be shown before `more` is
  dropped.
- **Tuning:** the Elixir engine should set its Stack process's binary
  virtual heap, as above, from the start. GC pauses with large buffers
  in one process remain a risk to measure at phase 8, with larger
  buffers and many sockets, which this spike did not try.
- **Measuring:** harness 2 ran in a privileged container on macOS
  (`spike/Dockerfile` and `spike/h2.sh` on the spike branch). That is
  enough for unimpaired runs; impaired runs need a kernel with `ifb`, as
  the CI runners have.

The prototype is discarded with its branch, as the plan says. Phase 1
starts from the plan's design, not from this code.
