# ADR 0013: vendored smoltcp with fast recovery

## Status

Accepted.

## Context

smoltcp 0.14.0, the latest release, fast-retransmits the first missing segment
after three duplicate ACKs but has no partial-ACK handling (RFC 6582,
"NewReno"). When one window loses two or more segments, which is what a
bounded queue does when it overflows, the ACK for that fast retransmission is
partial: it advances only to the next hole. By then the window has usually
drained, so no more duplicate ACKs can arrive, and the sender waits for its
retransmission timer. smoltcp clamps that timer to at least 1 s (RFC 6298
2.4), so each burst loss stalls the stream for a second whatever the RTT, and
the timeout then resends every segment after the hole (#55).

Measured between two SmolNet stacks over a relay link, a 4 MiB transfer took
about 0.2 s with no loss or with isolated single losses, and 10–25 s when the
link dropped bursts of 2–10 segments, with one stall of about 1 s per burst.

The retransmission state lives inside smoltcp's TCP socket and is not
reachable through its public API, so SmolNet cannot work around it from the
outside. A receiver-side trick, such as duplicating ACKs, would only help when
the sender is also smoltcp. Egress backpressure (#54) removes the losses that
a SmolNet link itself causes, but not loss on a real network.

## Decision

Vendor the published smoltcp 0.14.0 crate under `native/vendor/smoltcp` and
carry a small, self-contained loss-recovery patch on top of it.

- The first commit adds the crates.io package unmodified, except that it
  leaves out the registry's `.cargo-ok` marker and the crate's `Cargo.lock`.
  `.cargo_vcs_info.json` records the upstream commit. Every later change to
  the directory is a SmolNet patch and is reviewable as a diff against that
  commit.
- `smolnet_core` depends on the copy by `path`, with the version still pinned
  at `=0.14.0`. A `[patch.crates-io]` override was rejected: Rustler watches
  only declared path dependencies for changes, so an override would leave a
  source checkout loading a stale NIF after an edit to the vendored code. The
  fuzz crate reaches the copy through `smolnet_core`.
- The vendored crate is excluded from the native workspace, so its own test
  suite runs from its manifest. `mix precommit` runs that suite's library
  tests, which include the tests added for the patch.

The patch, in `src/socket/tcp.rs`, implements RFC 6582's partial-ACK rule:

- The third duplicate ACK records `recover`, the highest sequence number sent,
  when it starts a fast retransmission.
- An ACK that advances but stays below `recover` retransmits the next
  unacknowledged segment immediately and discards any RTT sample in progress
  (Karn's algorithm). An ACK at or above `recover` ends recovery and cancels
  any resend still queued from an earlier partial ACK.
- Duplicate ACKs during recovery do not start another fast retransmission, and
  a retransmission timeout abandons recovery in favour of its go-back-N resend.

SmolNet enabled no smoltcp congestion controller when this was written, so
the RFC's congestion window inflation and deflation rules would have had no
effect and were left out. It has since enabled CUBIC (see "Later
configuration"), whose own recovery rules now apply alongside the patch.
The patch left the 1 s minimum RTO unchanged; #103 later lowered it to
200 ms (see "Later patches").

## Verification

Five smoltcp unit tests cover a partial ACK resending the next hole, duplicate
ACKs below `recover`, a full ACK cancelling a resend that a partial ACK queued
in the same poll, a timeout during recovery, and re-entering recovery after a
full ACK. All five fail with the partial-ACK and re-entry logic removed.
smoltcp's other library tests still pass.

`test/smol_net/tcp_loss_recovery_test.exs` sends 256 KiB between two stacks
over a link that drops bursts of first-transmission data segments. It asserts
that each dropped segment is resent exactly once. Against unpatched smoltcp,
the same run resent 112 segments for 14 drops and took 7.4 s; with the patch,
it resends 14 and completes in about 30 ms. The assertion counts segments, not
time, so a slow runner does not change its outcome.

## Consequences

A multi-segment loss within one window is now repaired at one segment per
round trip instead of costing a 1 s timeout per burst. Losses that leave no
later segment to produce duplicate ACKs, such as the tail of a transfer, still
wait for the minimum RTO, 1 s when this was written and 200 ms since #103. Many holes in one window over a long RTT still
recover more slowly than SACK-based recovery would, since smoltcp's sender
does not use SACK. (#119 later added SACK-based recovery, and #138 tail
loss probes and RACK's detection of lost retransmissions; see "Later
patches".)

SmolNet now owns a fork of smoltcp's TCP sender. Upgrading smoltcp means
re-vendoring the new release and reapplying or dropping the patch, as
`MAINTAINING.md` describes. The patch is written to be offered upstream. Once
a smoltcp release carries equivalent recovery, the copy should be removed and
the registry dependency restored.

Hex packages are unaffected: they ship only precompiled NIFs and omit
`native/`.

## Later patches

- #80: upstream `dispatch` cleared a queued fast retransmission before
  handing it to the device. If the device refused it, as SmolNet's device
  does once a link's egress credit runs out part-way through a poll, the
  resend was lost with nothing left to schedule it, and a window already in
  flight stalled for good. The flag is now cleared only once `emit`
  succeeds, for both the third duplicate ACK and partial ACKs. Two unit
  tests cover a refused retransmission of each kind.
- #102: upstream's Nagle's algorithm held back any segment shorter than the
  MSS while any data was unacknowledged, so the partial tail of every write
  longer than an MSS waited for the ACK of the write's full segments: a
  round trip, plus the peer's delayed ACK, which Linux peers apply to a lone
  full segment. The socket now records the end of the last partial segment
  it sent and holds another only while that one is unacknowledged
  (Minshall's variant, as in Linux). `test_nagle` and three payload-size
  tests that relied on the old hold now expect the tail, and a new test
  covers a write longer than the MSS after a partial segment was ACKed.
- #103: upstream counted an ACK as a duplicate only if its window was
  unchanged, following RFC 5681. A Linux receiver grows its window on
  nearly every ACK while its receive buffer autotunes, so against Linux
  none of its duplicate ACKs counted, fast retransmit never started, and
  every loss waited for the 1 s retransmission timer. An ACK whose SACK
  blocks report data above the cumulative ACK that no earlier ACK
  reported is now a duplicate whatever its window, as RFC 6675 defines
  one. Without SACK, the window rule still applies. Two unit tests cover
  duplicates with a growing window and a window update that repeats an
  earlier SACK block.
- #103: the minimum RTO is 200 ms, Linux's `TCP_RTO_MIN`, instead of the
  1 s that RFC 6298 (2.4) says it SHOULD be. This departs from the RFC on
  purpose. The 1 s floor dates from coarse timers and long, variable round
  trips; on the tens-of-milliseconds paths SmolNet mostly runs over, it
  costs a whole second for every loss that fast retransmit cannot repair,
  such as a lost retransmission or the last segments of a transfer, and the
  backoff doubles from there. Under netem's Gilbert-Elliott burst loss it
  was most of SmolNet's shortfall against Linux. The RTO computed from
  SRTT and RTTVAR still applies above the floor, the initial RTO before any
  sample is still 1 s, and a timeout still doubles it. A spurious timeout
  costs a go-back-N resend and, with CUBIC, a window of one segment; Linux
  runs with the same floor. `test_rtt_estimator_min_rto` covers the floor
  and the backoff from it.
- #123: upstream's CUBIC started every connection from 2,048 bytes, two
  segments of its 1,024-byte default MSS, and `set_mss` never scaled it: a
  1,460-byte path started from 1.4 segments, a jumbo one from less than
  one. A short transfer over a round trip of tens of milliseconds finished
  in slow start, and a 1 MiB TLS upload to speed.cloudflare.com ran at
  0.4 times the kernel's speed, against 0.97 with no controller. `set_mss`
  now sets the RFC 6928 initial window, `min(10 * MSS, max(2 * MSS,
  14600))`, as Linux does, unless the connection has already had a loss or
  a timeout: a retransmitted SYN leaves the loss window, as RFC 6928 (2)
  requires. Reno has its own `set_mss` and is not enabled, so it keeps the
  upstream start. `mix precommit` now runs the library tests with
  `socket-tcp-cubic` enabled, so that CUBIC's own tests run: two new ones
  cover the window for three MSS values and the no-raise cases.
- #119: upstream's sender ignored SACK blocks when choosing what to resend,
  so the patch above repaired one hole per round trip, and a window that
  lost many segments took many round trips or a timeout to recover. The
  sender now keeps RFC 6675's scoreboard, in
  `src/socket/tcp/scoreboard.rs`: the ranges above the cumulative ACK that
  the remote has SACKed, up to 32 of them, forgetting the lowest when full.
  During fast recovery with a SACK-capable peer, `pipe` (the octets not
  SACKed and not deemed lost, plus those resent) stands in for the flight
  size against the congestion window, and every time the window has room
  the sender resends the next hole that RFC 6675's `IsLost` deems lost, or
  sends new data, or, with no new data left, resends the next hole anyway
  (`NextSeg` rules 1 to 3). So several holes are resent in one round trip.
  Recovery starts on the third duplicate ACK, or earlier once `IsLost`
  holds for the first unacknowledged octet; `recover` is still the recovery
  point, and ends it as before. A duplicate ACK no longer inflates the
  congestion window during SACK recovery, because `pipe` already leaves out
  what it SACKed; CUBIC's reduction on entry is otherwise unchanged. A
  partial ACK resends the segment it points at only if SACK recovery has
  not resent it already, and a fast retransmission stops short of SACKed
  data. The scoreboard drops ranges as the cumulative ACK passes them, and
  is cleared by a retransmission timeout, whose go-back-N resend repeats
  SACKed data anyway, and when the socket resets. An ACK that SACKs a range
  below one reported earlier now counts as a duplicate too, which the #103
  patch's high-water mark missed (#115). Without SACK, recovery is the
  NewReno patch above. Seven scoreboard unit tests, and seven socket tests
  that fail without the change, cover merging and trimming, `IsLost`,
  three holes resent at once, new data sent while `pipe` is below CUBIC's
  window, the #115 case, a partial ACK that must not resend, rule 3, a
  fast retransmission beside SACKed data, and the reset on a timeout.
- #128: upstream dropped every ICMP error it had no ICMP socket for
  (`_ => None` in `process_icmpv4` and `process_icmpv6`), and a
  connection's segment size was fixed at the handshake. With Don't
  Fragment on every IPv4 packet, a hop narrower than the interface's MTU
  refused every full-size segment, and the sender resent it forever. The
  patch adds path MTU discovery for TCP (RFC 1191, RFC 8201):
  - `process_icmpv4` handles "Fragmentation Needed" itself, before
    `Icmpv4Repr::parse`, which refuses the truncated packet such an error
    quotes (the quoted header's total length exceeds what is quoted). It
    checks the ICMP checksum and the quoted header, and takes the next-hop
    MTU, or for a zero one the RFC 1191 plateau below the quoted packet's
    length. `process_icmpv6` gets an arm for "Packet Too Big". Errors about
    anything but TCP are left as before, as are ICMP sockets.
  - Both discard an error whose MTU is not below the quoted packet's
    length, and pass the rest to `InterfaceInner::process_tcp_path_mtu`
    (`src/iface/interface/tcp.rs`), which requires the quoted source to
    be an interface address, reads the ports and sequence number from the
    first eight octets of the quoted TCP header, and offers the error to
    each TCP socket in turn.
  - `tcp::Socket::process_path_mtu`, the hook, holds the state the
    interface cannot reach. It ignores another connection's error. It
    raises the MTU to the family's floor: 576 for IPv4, whose MSS is
    TCP's default of 536 (RFC 1191 allows 68, but with Don't Fragment set
    a narrower path is a black hole whatever the floor, and the floor
    bounds what a forged error can do; Linux's `min_pmtu` is 552), and
    1280 for IPv6 (RFC 8201 4). An MTU not below what the connection sends
    changes nothing. A lower one must quote a sequence number in
    `SND.UNA..SND.NXT` of a connection in a data-sending state (RFC 5927
    4.1). The socket then keeps it as `path_mtu`, which `seq_to_transmit`
    and `dispatch` take the smaller of with the interface's MTU. The SYN's
    MSS still comes from the interface's MTU alone.
  - A reduction resends from `SND.UNA` at once, as a timeout's go-back-N
    does, since `dispatch` segments from the send buffer and so resends at
    the new size. It clears SACK and fast recovery state and Nagle's
    record of a small segment in flight, and discards the RTT sample, but
    does not call the congestion controller's `on_rto`: the drop was not
    congestion. The controller's `set_mss` gets the new MSS.
  - `Interface::path_mtu_stats` counts the errors received about TCP,
    those rejected, and the reductions, for SmolNet's `stack_info`.
  - The path MTU is the connection's own and is forgotten with it:
    smoltcp has no destination cache, so each new connection learns it
    again from its first full-size flight. `SND.NXT` is `remote_last_seq`,
    which a reduction or a timeout rewinds, so an error reporting a still
    lower MTU for a segment of the old flight is out of the window and
    ignored. If the path narrows further, the resend draws a fresh error.
  - Eight interface tests, five for IPv4 and three for IPv6, and a socket
    test cover the resend at the new size, errors quoting data not
    in flight, never raising, both floors, the plateaus, and implausible or
    corrupt errors. They fail without the patch, which dropped the errors.
- #132: upstream's `timeout` aborted a connection once the remote had
  sent nothing for that long, whether or not anything was outstanding,
  so it also ended healthy idle connections, and SmolNet cleared it once a
  connection was established. Nothing then bounded a connection with data
  outstanding: the RTO backs off to 60 s and nothing counts retries, and
  `rewind_zero_window_probe` backs the persist timer off to the same cap,
  so a vanished peer was retransmitted to forever. `timed_out` and
  `poll_at` now apply the timeout only while the connection waits on the
  remote (`awaits_remote`): in SYN-SENT and SYN-RECEIVED, with a FIN
  unacknowledged (FIN-WAIT-1, CLOSING, LAST-ACK), with the send buffer not
  empty, which covers unacknowledged data and data a zero window holds
  back, or with keep-alive that has no probe limit, as upstream's
  documentation of `set_timeout` describes. That is RFC 5482's user
  timeout, as Linux's `TCP_USER_TIMEOUT`, and bounds persist probing
  without more: a remote that answers the probes refreshes
  `remote_last_ts`, one that has gone does not. `close()` on a connection
  with nothing to send restarts the count, as `send_impl` already did for
  data, so a FIN after a long idle is not aborted at once. A socket
  aborted by it, or by keep-alive, reports `aborted_by_timeout()` until it
  is reused, from which SmolNet reports `:etimedout`.
- #133: upstream's keep-alive had one interval, for the first probe and
  every later one, and no count: the abort was the timeout's. `KeepAlive`
  now has Linux's three: `idle`, how long nothing arrives before the first
  probe; `interval`, between unanswered probes; and `probes`, how many go
  unanswered before the connection is aborted, when the next falls due.
  `set_keep_alive_config` sets them, and arms the first probe `idle` after
  the last packet received, not at once; any packet received resets the
  count, and the next probe is `idle` after it. A probe is sent only while
  the timer is idle, so never while data is outstanding, which the user
  timeout covers. The keep-alive timer is no longer rewound by packets
  other than probes, so the idle time counts from the last packet
  received, as on Linux. `set_keep_alive(interval)` keeps upstream's
  meaning, `idle` and `interval` both `interval` and no limit, so the
  timeout still ends it. Unit tests cover the abort after the user timeout
  and not before, with the retransmissions between, an idle connection a
  day later, a FIN after an idle, a zero window answered for 2000 s and
  then given up on, Linux's timing of nine probes and the abort, an answer
  that resets them, unacknowledged data left to the user timeout with
  keep-alive on, and keep-alive turned off. `test_established_timeout`
  now expects no timeout while the connection is idle.
- #138: with SACK recovery (#119), a single stream under burst loss still
  spent most of its time waiting on the retransmission timer. A loss at
  the tail of a flight left too few segments after it to draw three
  duplicate ACKs; a lost retransmission was resent only by the timer; and
  recovery took no round trip samples (Karn's algorithm), so a backed-off
  RTO stayed high. The patch adds RACK-TLP (RFC 8985), in
  `src/socket/tcp/rack.rs`, beside RFC 6675's `IsLost` rather than in
  place of it:
  - `SendLog` records when each octet from `SND.UNA` up was last sent.
    `dispatch` segments the send buffer afresh on every transmission and
    keeps no per-segment record, so the log keeps ranges instead: octets
    first sent in the same millisecond share one, up to 64, and past that
    the two neighbours sent closest in time merge as if sent at the later
    time, which can only delay a verdict on them. Each range says whether
    its last transmission was a retransmission, whether it is deemed lost,
    and whether it resent octets not deemed lost, which `pipe` counts
    twice. It also numbers the sends that made it, one or a run of
    consecutive sends of new data: SmolNet's clock counts milliseconds,
    and a resend and new data sent in the same one must still be told
    apart, or RACK would take the resend as lost when the new data sent
    before it arrived. `pipe` and `NextSeg` rule (1) now read the log: rule (1) resends
    the lowest octets not SACKed whose last transmission is deemed lost,
    by `IsLost` or by RACK, which may be a lost retransmission below
    `HighRxt`. `IsLost` marks only first transmissions lost. Rule (3) is
    unchanged.
  - RACK: each ACK finds, among the octets it newly reports delivered,
    cumulatively or by SACK, the latest sent, and takes its round trip as
    `RACK.rtt`. A retransmission acknowledged sooner than the minimum
    round trip is passed over, since its first transmission may be what
    arrived. A range sent before the latest delivered, and not SACKed, is
    lost once `RACK.rtt`, but at least SRTT, plus the reordering window
    has passed since it was sent. Sent before means in an earlier send, or
    earlier in the same run. The window is a quarter of SRTT, times a
    multiplier that a round trip with a D-SACK raises for the next 16
    recoveries, and at most SRTT. This departs from RFC 8985 6.2, whose
    window is a quarter of the minimum round trip, and zero during
    recovery or with three segments SACKed until reordering is seen,
    because RACK stands alone there. Here `IsLost` still marks what
    duplicate ACKs show, so RACK can keep a margin. With the RFC's window,
    under netem `delay 20ms reorder 10%` a single stream fell from 44 to
    9 Mbit/s: in the connection's first second RACK resent 140 to 730
    segments that had only been held back, every one D-SACKed, before any
    sign of reordering, and each recovery cut CUBIC's window, which
    smoltcp has no undo for. The minimum round trip is no base on such a
    path, since the packets let through first drag it down, and a
    reordered ACK can make `RACK.rtt` as short. A reordering timer
    rechecks when the next
    range would be lost. Data RACK deems lost starts fast recovery as the
    third duplicate ACK does, with the controller's usual reduction.
  - Tail loss probes (RFC 8985 7): to a peer that SACKs, with no
    recovery under way, nothing SACKed and nothing resent in the log,
    sending new data or an ACK of new data arms a probe timer: twice SRTT
    plus 2 ms, Linux's floor, and at least 10 ms, the TLP draft's, plus
    200 ms when only one segment is in flight, for the peer's delayed
    ACK, and only if it would expire before the RTO. Where the round trip
    is well under a millisecond, the 10 ms keeps an ACK that the host
    holds up briefly from drawing a needless probe, which
    `tcp_loss_recovery_test.exs` saw on a slow CI runner. The probe is one new segment if the peer's window allows one,
    and otherwise a resend of the last segment sent, whatever the
    congestion window; it restarts the RTO. Its SACK is what lets RACK
    find the losses before it. A probe that resent data and whose episode
    ends with no D-SACK repaired a loss, so the controller's `on_loss`
    then reduces the window (RFC 8985 7.4); recovery started meanwhile
    ends the episode instead.
  - Round trip samples: an ACK that newly reports delivered octets sent
    only once gives a sample from the earliest sent of them, as Linux
    takes one: from those acknowledged cumulatively, unless the ACK also
    acknowledges a retransmission so, when they may have arrived long
    before; otherwise from those newly SACKed. The estimator takes it
    during recovery, which holds back the cumulative ACK the timer's own
    sample waits for, and whenever that sample is not running, as after a
    retransmission. A backed-off RTO then resets once recovery shows the
    path works, instead of when new data sent after it is acknowledged.
  - RFC 3042 limited transmit: each of the first two duplicate ACKs
    before recovery lets one more new segment past the congestion window.
  - A timeout and a path MTU reduction (#128) both resend from `SND.UNA`:
    the log then deems everything in flight lost, as RFC 8985 6.3 has for
    a timeout, RACK's timer and the probe timer stop, and any probe
    episode ends, so neither fires on the resend. The retransmission
    timer a reduction leaves running (#142) is unchanged. Neither a probe
    nor a RACK resend is anything heard from the peer, so the user
    timeout (#132) still aborts only a connection whose peer is silent.
  - Ten socket tests cover a tail loss recovered through a probe, a probe
    of new data, a lost retransmission resent before the timeout, the
    backoff reset by a sample during recovery, reordering within RACK's
    window left alone, the reordering timer, recovery RACK starts under a
    zero window, limited transmit, a resent probe's congestion response
    with and without a D-SACK, and a path MTU reduction stopping the
    timers. Eight fail with RACK, the probes, the
    new samples and limited transmit switched off; the other two guard
    against spurious resends and pick the probe's segment. Eleven unit tests
    cover the log and RACK's state, and one the scoreboard's new query.
- #126: upstream's CUBIC grew the window in slow start by at most one MSS
  per ACK (`len.min(mss)`), RFC 5681's conservative rule, and counted the
  same capped `segment` in congestion avoidance, in both `W_est` and the
  cubic increment. Real servers acknowledge several segments at once:
  their NICs coalesce what arrives (GRO, LRO), and they delay ACKs. A
  capture of SmolNet's uploads to speed.cloudflare.com from a GitHub
  runner (integration run 36404710012) held 3,366 ACKs of new data,
  covering a median of 3 segments and a mean of 4.5; 4% covered one. In
  slow start SmolNet's flight grew about a quarter each round trip, 14,
  17, 20, 23 and 27 KB, where the kernel's doubles, and its uploads ran at
  0.52 (one stream) and 0.47 (four) times the kernel's.
  - Slow start now grows by every byte an ACK acknowledges, up to
    `ssthresh`, as Linux's `tcp_slow_start` does: RFC 3465's appropriate
    byte counting without its per-ACK limit. This departs from that
    limit, `L = 2 * SMSS`, on purpose: with ACKs of three segments it
    would still grow the window only about 1.6 times a round trip. The
    limit guards against bursts from a cumulative ACK of much data at
    once; the window still grows by no more than the ACK frees, so a
    burst is at most twice what was acknowledged, as on Linux.
  - After a timeout it counts one MSS per ACK, RFC 3465's `L = 1 * SMSS`,
    until the window reaches `ssthresh` or a loss ends it: the go-back-N
    resend follows a cleared scoreboard, so one ACK may cover everything
    the receiver held.
  - Congestion avoidance counts bytes too, in `W_est` (RFC 9438 4.3's
    `segments_acked`) and in the cubic increment, as Linux counts `acked`
    in `tcp_cong_avoid_ai`. Before, a peer that acknowledged every second
    segment halved CUBIC's growth, which #119's captures under loss
    showed. The increment's product is taken in 64 bits, since an ACK can
    now cover a whole window.
  - Two unit tests fail without the patch: stretch ACKs doubling the
    window in slow start and stopping at `ssthresh`, and ACKs of one, two
    and eight segments growing congestion avoidance alike, on the
    Reno-friendly line and on the curve after a loss. A third covers one
    MSS per ACK after a timeout.

## Later configuration

Changes to which of the vendored crate's features SmolNet enables, rather
than to its code:

- #103: `socket-tcp-cubic` is enabled, so every TCP socket runs CUBIC
  congestion control (RFC 9438), as Linux does by default. Before, no
  controller ran: a sender put its whole send buffer into the network each
  round trip, however narrow the path. Through a 20 Mbit/s bottleneck with a
  50-packet queue, that lost about half of every window, and SmolNet sent
  at 1.5 Mbit/s against the kernel's 19. Upstream's CUBIC started from two
  segments, far below Linux's ten, which #123 then patched (see "Later
  patches"). smoltcp's CUBIC ends its fast recovery
  on the first ACK of new data, a partial ACK included, and deflates the
  window to `ssthresh` there, while the patch's `recover` keeps
  retransmitting the remaining holes.
