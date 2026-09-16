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
