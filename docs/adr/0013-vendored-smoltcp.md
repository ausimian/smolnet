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

SmolNet enables no smoltcp congestion controller, so the RFC's congestion
window inflation and deflation rules would have no effect and are left out.
The 1 s minimum RTO is unchanged.

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
wait for the 1 s minimum RTO. Many holes in one window over a long RTT still
recover more slowly than SACK-based recovery would, since smoltcp's sender
does not use SACK.

SmolNet now owns a fork of smoltcp's TCP sender. Upgrading smoltcp means
re-vendoring the new release and reapplying or dropping the patch, as
`MAINTAINING.md` describes. The patch is written to be offered upstream. Once
a smoltcp release carries equivalent recovery, the copy should be removed and
the registry dependency restored.

Hex packages are unaffected: they ship only precompiled NIFs and omit
`native/`.
