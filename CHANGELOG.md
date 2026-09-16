# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- %% CHANGELOG_ENTRIES %% -->

## Unreleased

### Added

- Establish the Elixir application, Rustler NIF workspace, quality tooling, and
  cross-platform compatibility workflow.
- Add serialized single-feeder raw IPv6 ingress, transport-neutral egress
  messages, native stack polling, BEAM-owned timer scheduling, link-down
  policies, and stack metrics.
- Add stable socket identities, bounded one-shot readiness, independent
  read/write waiters, exact cancellation, and stale-event protection.
- Add bounded IPv6 TCP sockets with bind and ephemeral ports, nonblocking
  connect/finalize readiness, stable connection errors, endpoint queries,
  timer-driven retransmission and timeout, and abortive close.
- Add bounded IPv6 TCP stream send and receive, finite and infinite caller-side
  deadline loops, half-close behavior, EOF and reset handling, and graceful
  close with bounded native FIN retention.
- Add an outbound IPv6 `gen_tcp` backend with passive and bounded active modes,
  raw, line, and length-prefixed framing, binary/list representation, socket
  ownership transfer, adapter deadlines, and isolated lifecycle cleanup.
- Add bounded reusable IPv6 TCP listeners, low-level and `gen_tcp` accept,
  stable accepted-child identities, inherited inet options, explicit backlog
  overflow behavior, and race-safe listener, queue, and half-open cleanup.
- Add IPv4 and dual-family TCP parity across raw packet admission, stack
  addresses and routes, low-level clients and listeners, and the `gen_tcp`
  adapter, with explicit family isolation and IPv4 fragment rejection.
- Add bounded IPv6 UDP sockets with atomic datagram send/receive, source and
  destination metadata, connected-peer filtering, shared readiness and
  cancellation, and a supervised `gen_udp` adapter with passive, active, and
  controlling-process behavior.
- Add IPv4 UDP parity across low-level and `gen_udp` APIs, including
  family-specific MTU limits and checksum handling, dual-family protocol
  coexistence, and an explicit TCP/UDP inet option contract.
