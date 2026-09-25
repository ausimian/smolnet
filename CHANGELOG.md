# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- %% CHANGELOG_ENTRIES %% -->

## 0.5.0 - 2026-09-25

### Added

- A link can now bound the egress a stack hands it. Start the stack with
  `egress_credit: {packets, bytes}` and grant more with
  `SmolNet.grant_egress/3` as the link forwards packets. The stack never
  sends a batch the credit does not cover. What it cannot send waits in the
  sockets instead: TCP data in the send buffer and UDP datagrams in the
  transmit ring, so senders slow down as they would for a slow peer, and a
  link with a bounded queue no longer has to drop TCP data and wait out
  retransmissions. A stack waiting for credit does no work until the next
  grant. Stacks started without the option behave as before, and
  `SmolNet.Loopback` grants back what it forwards when given one.
- A stack can now hold more than 64 sockets. Start it with
  `limits: %{sockets: n}`, up to 512, to raise the limit; the default stays
  64. The limit counts the same native sockets as before: one per socket,
  listener pool member, and wildcard-UDP address, including TCP sockets still
  in TIME-WAIT. A stack whose sockets close first sustains about `n / 10` new
  connections per second. Each slot keeps its socket's buffers until it is
  freed, 128 KiB for a TCP socket at the default buffer sizes, so 512 TCP
  sockets can hold about 64 MiB. Whatever the limit, a stack's socket
  buffers total at most 128 MiB, as much as 64 sockets with the largest TCP
  buffers held before; an open past that returns `{:error, :system_limit}`.
  `SmolNet.stack_info/1` reports the total as `socket_buffer_bytes`.

### Changed

- The `ready_events` limit no longer caps how many sockets and blocked
  operations a stack holds. It used to admit fewer sockets than its value and
  at most that many waiting operations, so lowering it could make opens, and
  `:nowait` sends, receives, accepts and connects, fail with `:system_limit`.
  It now bounds only how many readiness events one native call delivers, and
  `sockets` governs capacity.

### Fixed

- A stack under sustained readiness load could leave a blocked operation on a
  socket opened later than most others waiting indefinitely. When more
  sockets became ready in one call than the stack could queue, each overflow
  restarted the scan for ready sockets from the beginning, so a scan that
  overflowed on every call never reached the later sockets. A raised
  `sockets` limit made this easier to hit.

## 0.4.2 - 2026-09-25

### Fixed

- A TCP stream that loses several segments from one window now resends just
  the lost segments, one round trip apart, instead of stalling for smoltcp's
  1 s minimum retransmission timeout and then resending everything after the
  first gap. Over loopback, a 4 MiB transfer through a link that drops bursts
  of segments fell from 10–25 s to about 0.2 s. A loss at the very end of a
  transfer can still wait for the 1 s timeout.
- A TCP socket that closes first now frees its socket slot as soon as
  TIME-WAIT ends, about 10 s after the connection closes. It used to hold the
  slot until the 30 s close deadline, so a stack whose sockets closed first
  could sustain only about 2 new connections per second before opens failed
  with `:system_limit`.

## 0.4.1 - 2026-09-25

### Changed

- The advertised TCP receive window now follows the socket's receive buffer
  (`rcvbuf`/`recbuf`) instead of being held to a single segment, so throughput
  over links with real latency is no longer capped at about one segment per
  round trip. Over a 50 ms round trip, a default 64 KiB buffer now reaches
  about 1 MB/s instead of about 47 KB/s. An embedder that relied on the
  smaller window can lower `recbuf` to get it back.

## 0.4.0 - 2026-09-24

### Added

- `SmolNet.monitor/1` returns an ordinary monitor on a stack, so a link
  receives a standard `:DOWN` message when its stack stops through
  `SmolNet.stop_stack/1` or a crash, and can exit instead of running its
  transport in front of a stack that is gone.

## 0.3.0 - 2026-09-24

### Added

- `SmolNet.ingress/2` accepts bounded packet lists, allowing burst-oriented
  links to cross the BEAM/native boundary and drive the stack once per batch.

## 0.2.1 - 2026-09-19

### Fixed

- Documentation guide links now resolve in ExDoc's Markdown output and the
  published package by keeping the guides at the package root.

## 0.2.0 - 2026-09-19

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

- The `:gen_tcp` and `:gen_udp` callback modules now follow their OTP address
  family names. Use `SmolNet.Inet.Tcp` and `SmolNet.Inet.Udp` with `:inet`, or
  `SmolNet.Inet6.Tcp` and `SmolNet.Inet6.Udp` with `:inet6`. These replace the
  former `SmolNet.InetBackend.Tcp4`, `SmolNet.InetBackend.Udp4`,
  `SmolNet.InetBackend.Tcp`, and `SmolNet.InetBackend.Udp` names, respectively.

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

## 0.1.2 - 2026-09-17

### Fixed

- Corrected release validation for production precompiled NIFs and their
  platform system-library dependencies.

## 0.1.1 - 2026-09-17

# Initial release

SmolNet embeds the Rust `smoltcp` network stack in an OTP-friendly Elixir
library with explicitly bounded native work.

### Added

- Independent, supervised IPv4 and IPv6 raw-IP stacks with configurable
  addresses, routes, MTUs, link behavior, and runtime telemetry.
- Low-level TCP and UDP sockets plus `:gen_tcp` and `:gen_udp` compatible
  adapters for clients, listeners, datagrams, active and passive delivery, and
  controlling-process ownership.
- Scheduler-aware NIF execution with bounded queues, buffers, readiness,
  maintenance, shutdown, and packet-processing work.
- Checksum-pinned precompiled NIFs for Linux x86_64, Linux AArch64, and Apple
  Silicon macOS, while repository checkouts continue to build from Rust source.
