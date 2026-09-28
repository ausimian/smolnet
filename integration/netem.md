# The netem matrix

`netem_matrix.exs` runs bulk TCP transfers under every `tc netem` profile
of `SmolNet.Integration.Soak.Netem`, with SmolNet at one end, and beside
them the same transfers between two kernel stacks on a path impaired the
same way. This page explains how the comparison is built and how to read
it, and records the latest results (#91).

```console
integration/netem-topology.sh isolate mix run integration/netem_matrix.exs --no-pcap
```

Run it with `MIX_ENV=prod` for the release NIF. It needs no sudo where the
host allows unprivileged user namespaces. Throughput is CPU-bound on the
profiles without delay, so pin it to idle cores with `taskset` and keep
the capture off.

## The kernel baseline

`--baseline` runs a scenario over the kernel's loopback, which never
crosses the TUN device, so under `--netem` it compares impaired SmolNet
with an unimpaired kernel (#86). The matrix instead gives its namespace a
second path, beside the device:

```
SmolNet --tun0-- this namespace --netem-h==netem-p-- netem-peer
```

- `netem-topology.sh up` adds a veth pair, `netem-h` here (10.80.0.1,
  fd00:80::1) and `netem-p` in the namespace `netem-peer` (10.80.0.2,
  fd00:80::2). The scenario opens the peer's listener there with
  `:gen_tcp`'s `netns` option, so the same VM drives both kernel ends, as
  it drives both ends of SmolNet's transfers.
- For each profile, `Netem.impair/2` shapes the device and `netem-h`
  alike: the profile's qdiscs on the egress and, through an `ifb`, on the
  ingress. Each path is impaired once each way, in the same namespace, by
  the same qdiscs.
- netem acts on packets as the qdisc sees them, and the kernel's TCP
  hands it GSO packets of up to 64 KiB, which are only split after it. A
  single netem loss would drop dozens of the kernel's segments, where it
  drops one of SmolNet's. So `up` sets `gso_max_segs 1` on the device and
  both veth ends, and `down` restores the device's. This is what #103 and
  #119 did by hand.
- The flows are `send` (SmolNet sends to the kernel here, through the
  device), `receive` (the kernel here sends to SmolNet) and `kernel` (the
  kernel here sends to the kernel in `netem-peer`).

Every transfer is random data checked by length and SHA-256 at each
receiver, split evenly over 1 or 4 connections at once. Its completion
time runs from the first connect to the last byte received.

The baseline is a fair one for the path, not for the endpoints. The
kernel keeps its autotuned buffers of up to 6 MiB, and its timestamps,
DSACK, TLP and RACK, where SmolNet has 256 KiB and none of those. Where the
path adds no delay, both stacks run as fast as the CPU lets them. SmolNet
then pays for its NIF and the TUN helper, and the kernel for a veth.

## The verdict

- **Integrity.** Every transfer, SmolNet's or the kernel's, must be
  delivered intact within `--transfer-timeout` (600 s), or the run fails.
- **Stalls.** A SmolNet transfer also fails when its receiver waits for
  data longer than `--stall-rtos` (5) retransmission timeouts in a row
  would take, each twice the last. The timeout is the profile's round
  trip plus the 200 ms minimum both stacks use. That limit is 6.2 s
  without delay, 7.4 s under `reorder`, 12.4 s under `delay` and
  `high-bdp`, and 38 s under `bufferbloat`. Receivers wait on past the
  limit, so each stall's whole length is recorded.
- **Stalls that are noted, not failed.** The kernel's own stalls are
  noted. So are SmolNet's under `loss-burst`, whose bursts outlast the
  retransmission timer for Linux too. netem's Gilbert-Elliott state moves
  on only as packets pass, so while a sender waits out its timer the burst
  goes on, and its lone retransmission meets it again. In 190 transfers of
  Linux sending to SmolNet (4 streams, IPv4), 11 waited longer than 6.2 s,
  the longest 53 s. In 89 of Linux sending to Linux, 6 did, the longest
  110 s. A capture of one of SmolNet's showed it acknowledging every
  retransmission that reached it within a millisecond. netem dropped the
  rest, and the acknowledgements, while Linux backed off to a 12.5 s wait.
- **Throughput.** Measured, not judged. A case whose median is below
  `--gap` (0.5) of the kernel's is noted as a gap, tagged with the issue
  that explains it (`NetemMatrix.triage/2`). A gap under a profile without
  delay that is no wider than the unimpaired one is tagged as the CPU's.

`verdict.json` holds every transfer (`results.transfers`) and a row per
case (`results.matrix`): medians, the extremes, the longest wait, the
ratio to the kernel and the triage.

## Results, 2026-09-28

The default run: every profile and `none`, 8 MiB transfers, 1 and 4
streams, IPv4 and IPv6, 3 interleaved repeats, 288 transfers in 36 min.

- **Build and host.** It ran on `main` at 3041d7e, with CUBIC, a 200 ms
  minimum RTO, IW10, 256 KiB buffers, the RFC 6675 SACK scoreboard, PMTUD
  and #132's user timeout. The host was a 20-core Intel NUC, Linux 6.12,
  with the run pinned to cores 16 to 19.
- **Contention.** A `mix precommit` ran on cores 4 to 11 during the first
  few minutes, over the first repeat of `none`, `bufferbloat` and
  `corrupt`. The medians match the later repeats.
- **Verdict: fail.** One SmolNet one-stream send under `loss-burst` (IPv6)
  did not finish in 600 s, which is #138. Every other transfer arrived
  intact, and no SmolNet stall outside `loss-burst` passed its limit.

Each cell is the median in Mbit/s, the median completion time, and
SmolNet's ratio to the kernel on the same profile and stream count.
The longest wait is the longest any receiver waited for data, over the
repeats. Triage: CPU means the profile has no delay and the gap is no
wider than unimpaired.

### IPv4

| profile | streams | SmolNet sending | SmolNet receiving | kernel⇄kernel | longest wait: send / receive / kernel | triage |
| --- | --- | --- | --- | --- | --- | --- |
| `none` | 1 | 714 (94 ms), **0.35×** | 479 (140 ms), **0.24×** | 2034 (33 ms) | 2 ms / 1 ms / 1 ms | CPU |
| `none` | 4 | 671 (100 ms), **0.14×** | 469 (143 ms), **0.1×** | 4793 (14 ms) | 4 ms / 10 ms / 4 ms | CPU |
| `bufferbloat` | 1 | 9.58 (7007 ms), **1.02×** | 9.55 (7026 ms), **1.01×** | 9.42 (7125 ms) | 32 ms / 42 ms / 32 ms |  |
| `bufferbloat` | 4 | 7.88 (8520 ms), **0.83×** | 9.62 (6976 ms), **1.01×** | 9.48 (7077 ms) | 846 ms / 175 ms / 155 ms |  |
| `corrupt` | 1 | 627 (107 ms), **0.33×** | 559 (120 ms), **0.29×** | 1917 (35 ms) | 2 ms / 1 ms / 4 ms | CPU |
| `corrupt` | 4 | 699 (96 ms), **0.21×** | 479 (140 ms), **0.14×** | 3355 (20 ms) | 11 ms / 8 ms / 2 ms | CPU |
| `delay` | 1 | 7.3 (9198 ms), **1.19×** | 6.53 (10.3 s), **1.07×** | 6.13 (10.9 s) | 209 ms / 197 ms / 180 ms |  |
| `delay` | 4 | 17.7 (3782 ms), **1.46×** | 9.87 (6800 ms), **0.81×** | 12.2 (5523 ms) | 294 ms / 281 ms / 297 ms |  |
| `duplicate` | 1 | 849 (79 ms), **0.44×** | 533 (126 ms), **0.28×** | 1917 (35 ms) | 1 ms / 1 ms / 1 ms | CPU |
| `duplicate` | 4 | 652 (103 ms), **0.2×** | 476 (141 ms), **0.15×** | 3196 (21 ms) | 8 ms / 9 ms / 4 ms | CPU |
| `high-bdp` | 1 | 9.1 (7371 ms), **0.33×** | 8.99 (7461 ms), **0.33×** | 27.5 (2438 ms) | 200 ms / 202 ms / 200 ms | #103 |
| `high-bdp` | 4 | 25.9 (2590 ms), **0.95×** | 24.9 (2691 ms), **0.92×** | 27.2 (2468 ms) | 217 ms / 221 ms / 218 ms |  |
| `loss-burst` | 1 | 0.26 (260 s), **<0.01×** | 42.1 (1594 ms), **0.77×** | 55 (1221 ms) | 180 s / 1.5 s / 1.5 s | #138, #139 |
| `loss-burst` | 4 | 20.5 (3280 ms), **0.02×** | 486 (138 ms), **0.56×** | 872 (77 ms) | 2.8 s / 645 ms / 209 ms | #138, #139 |
| `reorder` | 1 | 45.1 (1489 ms), **0.45×** | 17.8 (3767 ms), **0.18×** | 99.7 (673 ms) | 41 ms / 42 ms / 41 ms | #138 (send), #86 (receive) |
| `reorder` | 4 | 118 (567 ms), **1.8×** | 102 (660 ms), **1.54×** | 65.9 (1018 ms) | 43 ms / 45 ms / 42 ms |  |

### IPv6

| profile | streams | SmolNet sending | SmolNet receiving | kernel⇄kernel | longest wait: send / receive / kernel | triage |
| --- | --- | --- | --- | --- | --- | --- |
| `none` | 1 | 706 (95 ms), **0.36×** | 460 (146 ms), **0.23×** | 1974 (34 ms) | 1 ms / 1 ms / 1 ms | CPU |
| `none` | 4 | 678 (99 ms), **0.18×** | 447 (150 ms), **0.12×** | 3728 (18 ms) | 7 ms / 10 ms / 3 ms | CPU |
| `bufferbloat` | 1 | 9.45 (7102 ms), **1.02×** | 9.42 (7122 ms), **1.01×** | 9.29 (7223 ms) | 32 ms / 42 ms / 32 ms |  |
| `bufferbloat` | 4 | 7.84 (8564 ms), **0.84×** | 9.49 (7072 ms), **1.01×** | 9.35 (7176 ms) | 849 ms / 184 ms / 215 ms |  |
| `corrupt` | 1 | 559 (120 ms), **0.31×** | 469 (143 ms), **0.26×** | 1814 (37 ms) | 5 ms / 2 ms / 1 ms | CPU |
| `corrupt` | 4 | 569 (118 ms), **0.16×** | 463 (145 ms), **0.13×** | 3532 (19 ms) | 10 ms / 7 ms / 2 ms | CPU |
| `delay` | 1 | 7.36 (9113 ms), **0.93×** | 6.57 (10.2 s), **0.83×** | 7.94 (8452 ms) | 212 ms / 267 ms / 221 ms |  |
| `delay` | 4 | 17.1 (3930 ms), **1.8×** | 12 (5574 ms), **1.27×** | 9.5 (7062 ms) | 294 ms / 316 ms / 312 ms |  |
| `duplicate` | 1 | 746 (90 ms), **0.4×** | 479 (140 ms), **0.26×** | 1864 (36 ms) | 1 ms / 1 ms / 1 ms | CPU |
| `duplicate` | 4 | 546 (123 ms), **0.18×** | 457 (147 ms), **0.15×** | 3050 (22 ms) | 15 ms / 8 ms / 5 ms | CPU |
| `high-bdp` | 1 | 9.11 (7369 ms), **0.33×** | 9 (7457 ms), **0.33×** | 27.5 (2445 ms) | 201 ms / 202 ms / 200 ms | #103 |
| `high-bdp` | 4 | 25.9 (2594 ms), **0.95×** | 25 (2688 ms), **0.92×** | 27.2 (2469 ms) | 216 ms / 221 ms / 205 ms |  |
| `loss-burst` | 1 | 3.18 (21.1 s), **0.68×**, 2/3 done | 20.5 (3276 ms), **4.39×** | 4.67 (14.4 s) | 276 s / 26.5 s / 26.5 s | #138: the third timed out |
| `loss-burst` | 4 | 44.6 (1506 ms), **0.15×** | 476 (141 ms), **1.57×** | 302 (222 ms) | 1.2 s / 11 ms / 416 ms | #138, #139 |
| `reorder` | 1 | 44.6 (1504 ms), **1.08×** | 23.3 (2881 ms), **0.57×** | 41.2 (1628 ms) | 41 ms / 42 ms / 41 ms |  |
| `reorder` | 4 | 122 (551 ms), **1.29×** | 53.6 (1252 ms), **0.57×** | 94.1 (713 ms) | 46 ms / 46 ms / 42 ms |  |

### Triage

- **`delay`, `bufferbloat`: no gap.** SmolNet keeps level with the kernel,
  or ahead of it, both ways. With four streams into the 1 s bucket,
  SmolNet's sends wait up to 0.85 s for the queue to drain, where the
  kernel's wait 0.2 s. That is 0.83 of the kernel's rate.
- **`none`, `corrupt`, `duplicate`: CPU, not the impairment.** With no
  delay on the path, the kernel runs at 2 to 5 Gbit/s across a veth, and
  SmolNet at 450 to 850 Mbit/s through its NIF and the TUN helper. The
  ratios under corruption and duplication match the unimpaired ones, and
  no transfer waited more than 15 ms. SmolNet drops every corrupted
  segment by checksum (#63) and every duplicate, and the SHA-256 of every
  transfer matched. There is no distinct gap here to file.
- **`high-bdp`, one stream: the buffers (#103).** 256 KiB over a 200 ms
  round trip is about 10 Mbit/s, where the kernel's autotuned buffers
  reach 27. With `--buffer 1048576`, the most SmolNet takes, one stream
  ran at 22.5 Mbit/s sending and 22.1 receiving (IPv4, 3 repeats). The
  window scales (#49); only the default holds it back. Four streams are
  level with the kernel.
- **`loss-burst`, SmolNet sending: #138, then #139.** One stream is the
  largest gap in the matrix. Two of three IPv4 transfers took over 170 s,
  and one IPv6 transfer did not finish in 600 s, with single waits for
  data of 171, 180 and 276 s. Linux's worst on the same path was 26.5 s.
  A burst takes the lone retransmission after a timeout, the timer backs
  off, and nothing brings it back down, which is #138's backoff reset,
  TLP and RACK. With four streams, recovery holds but runs at 0.02 to
  0.15 of the kernel. #139 covers SACK recovery's use of the window and
  MSS.
- **`loss-burst`, SmolNet receiving: within the noise, #117 aside.** The
  medians run from 0.56 to 4.4 times the kernel's, whose own repeats
  swing widely: over IPv4, 31 to 74 Mbit/s with one stream and 293 to
  2,314 with four. The SmolNet receiver SACKs one block, and drops
  segments past its fourth hole (#117), so a Linux sender repairs fewer
  holes per round trip than against Linux. Its stalls are Linux's, as
  the verdict section shows.
- **`reorder`, one stream over IPv4: #86 receiving, #138 sending.** A Linux
  sender backs off when reordering looks like loss, and SmolNet sends no
  timestamps or DSACK to let it undo that (#86's finding). The SmolNet
  sender takes three duplicate ACKs as a loss, and so reordering as
  congestion, where RACK's reordering window (#138) would tell them apart.
  Both are narrower over IPv6, and with four streams SmolNet sending is
  ahead of the kernel. The kernel's own one-stream runs range from 22 to
  105 Mbit/s here.

No new issue was filed: every gap past 0.5 of the kernel is one of these.

## Internet peers

The TLS scenario against speed.cloudflare.com was run on GitHub's runners
under six of the profiles, applied to the runner's tun0. Each was a
3-minute run over IPv4 with `--concurrency 4`, dispatched on `main` at
3041d7e. Every run passed, with every body's length and SHA-256 intact and
no TLS alert or deadline missed.

The tls scenario's kernel comparison does not cross tun0, so its kernel
was not impaired. The table compares SmolNet under each profile with
SmolNet unimpaired, from the nightly run of 2026-09-28 (600 s, no
netem). The cells are median Mbit/s, one stream and then four.

| profile | run | download | upload | TLS handshake |
| --- | --- | --- | --- | --- |
| none (nightly) | [36374549907](https://github.com/ausimian/smolnet/actions/runs/36374549907) | 93 / 166 | 45 / 46 | 28 ms |
| `delay` | [36379216725](https://github.com/ausimian/smolnet/actions/runs/36379216725) | 5.9 / 13.6 | 2.6 / 3.3 | 514 ms |
| `high-bdp` | [36379652703](https://github.com/ausimian/smolnet/actions/runs/36379652703) | 7.5 / 16.8 | 3.2 / 4.0 | 417 ms |
| `bufferbloat` | [36379641500](https://github.com/ausimian/smolnet/actions/runs/36379641500) | 8.9 / 8.8 | 7.3 / 7.6 | 102 ms |
| `reorder` | [36379227804](https://github.com/ausimian/smolnet/actions/runs/36379227804) | 31 / 67 | 10.1 / 14.5 | 94 ms |
| `loss-burst` | [36379222442](https://github.com/ausimian/smolnet/actions/runs/36379222442) | 87 / 146 | 11.5 / 7.6 | 34 ms |
| `corrupt` | [36379647282](https://github.com/ausimian/smolnet/actions/runs/36379647282) | 86 / 160 | 21 / 27 | 29 ms |

- **`delay` and `high-bdp`: the window, then #126 for uploads.**
  Downloads fit the 256 KiB receive buffer over the added 200 ms round
  trip (#103), a ceiling of about 9 Mbit/s a stream before slow start.
  Uploads run at half that. They are about 1 MB each, so slow start is
  most of every transfer, and SmolNet's grows by at most a segment per
  ACK against Cloudflare's stretch ACKs (#126).
- **`bufferbloat`: the bucket.** Both ways run at the 10 Mbit/s the
  profile allows.
- **`loss-burst`, `corrupt` and `reorder`: uploads, #138 and #126.**
  Uploads, where SmolNet sends, fall to between a sixth and three fifths of
  their unimpaired rate. These are the same gaps as SmolNet sending in the
  local matrix. Downloads, where Cloudflare's Linux sends to SmolNet, keep
  most of their rate under `loss-burst` and `corrupt`. Under `reorder`
  they fall to a third or so, as the local matrix's receiving does (#86).
- **Handshakes** (TCP connect and TLS together) grow by about two and a
  half of the profile's added round trips, and no more.

## Idle connections under netem

`idle.exs` ran under `--netem delay`, `loss-burst` and `reorder`, in the
same kind of namespace. It used the pull request's shortened timers: a
45 s user timeout, and keepalive after the idle time below, then 3
probes 2 s apart. These timers are #132's and #133's (#148), so the
results depend on them.

| profile | run | verdict | quiet window | vanished peers |
| --- | --- | --- | --- | --- |
| `delay` | 4 min, keepalive after 50 s | fail: busy polling | 3.4 polls/s | not reached |
| `delay` | 6 min, keepalive after 150 s | pass | 0 polls/s | all 36 detected; `reboot` 1.8 to 17.7 s after the path returned |
| `reorder` | 4 min, keepalive after 50 s | pass | 0.1 polls/s | all 40 detected; `reboot` 0.9 to 9 s after the path returned |
| `loss-burst` | 4 min, keepalive after 50 s | fail: a 30 s exchange deadline | 0 polls/s | not reached |

- **`delay`, the first run: the timers, not SmolNet.** Over 200 ms round
  trips, opening all 240 connections, TLS included, took 80 s, so the
  first connections were idle past the 50 s keepalive time before the
  quiet window ended. Their probes are what polled: one 1-byte probe a
  connection, each answered. With keepalive at 150 s the window is silent.
  The pull request's timings assume connections open in a few seconds.
- **`loss-burst`: #138.** The last exchange of a TLS `stall` connection
  sent 256 KiB into its peer's small window. A burst then took one segment
  and each of its resends, 0.2, 0.4, 0.8 up to 12.8 s apart, so the
  exchange missed its 30 s deadline. That is the backoff of the matrix's
  one-stream sends, and Linux stalls on this profile too, for up to
  110 s. The idle scenario's deadlines are too short for this profile.
- Every vanished peer was detected under `delay` and `reorder`:
  - `unacked` and `nat` by the 45 s user timeout;
  - `silent`, and the TCP `zero_window` connections, by keepalive or the
    user timeout;
  - `reboot` by the RST.

## Repeating these runs

Prefix each with `MIX_ENV=prod taskset -c <idle cores>`:

```console
# the matrix
integration/netem-topology.sh isolate mix run integration/netem_matrix.exs --no-pcap

# high-bdp with SmolNet's largest buffers
integration/netem-topology.sh isolate mix run integration/netem_matrix.exs --no-pcap \
  --family inet --profiles high-bdp --flows send,receive --buffer 1048576

# stalls under loss-burst, Linux sending to SmolNet against Linux to Linux
integration/netem-topology.sh isolate mix run integration/netem_matrix.exs --family inet \
  --profiles loss-burst --flows receive,kernel --streams 4 --repeats 300 \
  --transfer-timeout 120000 --duration 15m

# idle connections
integration/pmtu-topology.sh isolate mix run integration/idle.exs --netem delay \
  --duration 6m --quiet 30s --idle-max 1m --trickle-max 10s --outage-max 20s \
  --nat-timeout 20s --warmup 2m --keepalive --user-timeout 45s \
  --keepalive-idle 150s --keepalive-interval 2s --keepalive-probes 3
```

The internet runs are dispatched as in `README.md`:

```console
gh workflow run integration.yml --repo ausimian/smolnet --ref main -f scenario=tls \
  -f duration=3m -f netem=<profile> -f family=inet -f extra_args='--concurrency 4'
```
