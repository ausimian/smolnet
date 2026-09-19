### Fixed

- The two affected test suites now distinguish liveness waits from timing
  assertions. Shared CI runners give operations that must eventually complete
  extra headroom without relaxing tests whose timeout is the behaviour under
  test.

- Fixed keep-alive probes and other challenge ACKs never being sent. The stack
  passed the raw BEAM monotonic clock, which is negative, to smoltcp as its
  instant; smoltcp's challenge-ACK rate limiter compares the instant against a
  timer that starts at zero, so the gate never opened and a peer's TCP keep-alive
  probe went unanswered — iOS, for one, resets an idle connection after three.
  The clock now counts milliseconds since the VM started.

- Raw-mode `:gen_tcp` streams are no longer bounded by `packet_size` and the
  receive buffer. A passive `recv/3` that names its length now accumulates to that
  length however large, and `send/2` of any size is accepted and written in the
  bounded pieces the adapter already used. Both previously returned `:emsgsize`
  and closed the socket, which `:gen_tcp` never does for a raw stream. Chunk
  reads (`recv(socket, 0)`), active delivery, and framed packet modes keep their
  bound.

- Fixed a `:gen_tcp` receive that could fail with `:busy` against its own socket.
  When a passive exact-length read got part of its data together with a select
  for the rest, the adapter immediately asked the stack for the remainder, which
  the stack rejected because that read's waiter was already armed. The caller's
  read failed `:busy`, and the stray waiter made every later read fail the same
  way until data happened to arrive. The adapter now waits for the select it
  already holds.

- Fixed a bug where a call to `SmolNet.ingress/2` could hang. If the stack was
  still busy with earlier work when a packet arrived, it held the packet and
  planned to process it after its next poll. A socket call made at the same
  time could cancel that poll. The stack then never processed the held packet,
  and the link process that sent it waited forever, or until an unrelated
  timer happened to fire. The stack now processes the held packet even when
  the poll it was waiting for has been cancelled.

### Added

- Raw-IP links now receive bounded egress batches. Each native output envelope
  is delivered as one `{:smol_stack, link_ref, :egress, packets}` message,
  allowing links to amortize mailbox handling and transport writes while
  preserving packet order.

- `SmolNet.Loopback`, a link process that feeds every packet its stack emits
  back into that same stack. One stack then reaches its own addresses with no
  peer, no external transport, and no privileges, which makes a runnable
  example or test out of what previously needed two stacks and a relay. The
  link takes the `SmolNet.start_stack/1` options apart from `:egress`, which it
  supplies, owns the stack it loops, and is stopped by `SmolNet.stop_stack/1`.
  `examples/loopback.exs` runs a complete `gen_tcp` request and response over
  one stack.

### Changed

- `SmolNet.Loopback.start_link/1` now returns `{:ok, link, stack}`, exposing the
  newly created stack without a follow-up `SmolNet.Loopback.stack/1` call. The
  stack is also returned as child information when a loopback link is started
  under a supervisor.

- The README is now a concise introduction to SmolNet's motivation, raw-IP link
  boundary, and available interfaces. Detailed `:gen_tcp`, `:gen_udp`, and
  low-level socket usage now lives in dedicated ExDoc guides, while development
  and native-build material has moved to the repository-only maintainer guide.

- TCP receive and send buffers now default to 64 KiB and can be sized
  independently with socket-style `rcvbuf`/`sndbuf` at low-level open or inet
  `recbuf`/`sndbuf` when connecting and listening. Sizes from 1 KiB through
  1 MiB are supported, accepted sockets inherit listener sizes, and stack
  diagnostics report each socket's values.

- The NIF now charges `enif_consume_timeslice` incrementally at work-loop chunk
  boundaries instead of once at the end of a call, and stops early when a charge
  reports the caller's reduction slice as spent. A stack owner that has already
  used most of its slice before re-entering the NIF receives a shorter native
  slice, so other processes on the scheduler are not starved. The 750 microsecond
  work deadline remains a hard ceiling, at least one chunk always runs per call,
  and retained work still continues through the existing `more` protocol.
