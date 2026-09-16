# ADR 0005: IPv6 TCP stream I/O and caller-owned waits

## Status

Accepted for Phase 5.

## Context

TCP stream I/O must preserve the native stack's exclusive ownership and
atomic readiness registration while preventing Rust or `SmolNet.Stack` from
retaining arbitrary application payloads, receive accumulators, or deadlines.
The public API also needs deterministic EOF, reset, half-close, timeout, and
graceful-close behavior.

## Decision

- `tcp_send` copies at most the stack's `bytes_copied` limit and available
  fixed TX capacity. It returns the accepted byte boundary; Elixir constructs
  and retains the unsent sub-binary. Rust never stores the remainder.
- `tcp_recv` removes at most the requested size, the `bytes_copied` limit, and
  available fixed RX data. Exact-length accumulation is a caller-owned list of
  bounded binaries that is flattened once on completion or error.
- A blocked or bounded-partial nowait send returns
  `{:select, {select_info, remainder}}`. A blocked nowait receive returns
  `{:select, select_info}`; an exact receive with partial data returns
  `{:select, {select_info, partial}}`. If the per-call byte limit, rather than
  socket capacity, caused the partial operation, native code arms the waiter
  and marks it immediately ready so retry cannot be lost.
- `recv(socket, 0, timeout)` waits for data when none is available, then
  returns one currently available bounded chunk. It deliberately does not
  drain an unbounded stream in one public call.
- Finite and infinite connect, send, and receive calls are Elixir loops over
  the same nowait primitives. A finite call computes one absolute monotonic
  deadline and is limited to the BEAM receive range of `0..4_294_967_295`
  milliseconds. Timeout cancels the exact select reference and drains a
  racing matching notification. Every synchronous wait monitors the stack and
  maps stack loss to `:closed`.
- A send that times out or fails after accepting bytes returns
  `{:error, {reason, unsent_remainder}}`. An exact receive that times out or
  fails after reading bytes returns `{:error, {reason, partial_data}}`.
- Peer EOF returns already buffered bytes successfully, including a shorter
  final exact receive, before the next receive reports `:closed`. An inbound
  RST remains the distinct `:connection_reset` error.
- `shutdown(:write)` uses smoltcp's transmit-half close, emits and retransmits
  FIN, aborts a pending write waiter, and rejects later sends while preserving
  reads. `shutdown(:read)` is local policy recorded in the bounded socket
  entry; it aborts a pending read waiter and rejects later receives. Repeating
  either shutdown is idempotent.
- `close/1` now supersedes ADR 0004's Phase 4 abortive behavior for established
  sockets. It invalidates the public identity and aborts its waiters
  immediately, but retains the bounded native TCP record long enough to send
  and retransmit FIN. Closing records count against the socket limit, use a
  bounded cursor-resumable maintenance sweep, and have a 30-second deadline
  measured from `close/1`. Pre-connect and failed sockets still close
  immediately.
- Send, receive, shutdown, and close all return bounded egress, readiness,
  continuation, and `poll_at` effects through the existing envelope. The stack
  owner forwards packets and replaces its timer exactly as for ingress and
  connect.
- Caller loops yield after 16 immediate retry hints. Native calls remain
  bounded independently by bytes, output packets, readiness events, and
  maintenance work.

## Consequences

IPv6 clients can use low-level TCP streams synchronously or with one-shot
select notifications without moving application operation state into Rust or
the stack GenServer. Native memory remains fixed per live or closing socket,
and closing sockets cannot be replaced by an unbounded number of new sockets.

The API still has no packet framing, active-mode policy, application send
queue, inet adapter, listener, IPv4 support, or UDP support. Those remain in
their planned later phases.
