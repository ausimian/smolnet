# ADR 0004: IPv6 TCP connect lifecycle

## Status

Accepted for Phase 4.

## Context

The first real sockets must reuse Phase 3's stable identities and atomic
try-and-arm readiness path while keeping every native call bounded. smoltcp
does not provide port allocation, retain a separate pre-connect bind state, or
distinguish every application-facing reason once a TCP socket reaches
`CLOSED`. Raw-IP link-local addresses also have no operating-system interface
index from which a zone can be inferred.

## Decision

- Each logical TCP socket owns one smoltcp TCP socket in the stack's shared
  `SocketSet`, with fixed 4096-byte receive and transmit buffers. Live sockets
  remain capped by the stack's `ready_events` limit, so aggregate native TCP
  buffer memory is bounded.
- IPv6 endpoints use `:socket`-style maps with `family`, `addr`, `port`, and
  optional zero `flowinfo` and `scope_id`. Addresses in `fe80::/10` require a
  positive 32-bit `scope_id`; other unicast addresses require scope zero.
  Multicast and remote unspecified addresses are rejected. Elixir validates
  first and the NIF independently validates the encoded address, port, and
  scope.
- Bind port zero and unbound connect allocate from the fixed
  `49152..50175` range using a per-stack rotating cursor. Phase 4 reserves a
  port across the complete stack, which is deliberately stricter than
  address-specific OS reuse. Collision returns `:address_in_use`; a full
  range returns `:ephemeral_ports_exhausted`.
- Connect checks that the remote address is on a configured interface prefix
  or matches a configured route before mutating the socket. Failure is
  `:network_unreachable`.
- Connect is initiate-or-finalize. Initiation records the selected local and
  remote endpoints, starts smoltcp, installs the Phase 3 write waiter, arms the
  socket send waker, drives bounded egress, and returns a select hint.
  Retrying after the one-shot notification returns `:ok`, a stable failure, or
  another select hint after a spurious wake. A competing connect waiter is
  `:busy`; a completed connection is `:already_connected`.
- TCP's native inactivity timeout is 30 seconds. It bounds an ignored
  handshake independently of the caller-side application deadline that Phase
  5 will add. Direct RST while `SYN-SENT` maps to `:connection_refused`; RST
  after establishment but before connect finalization maps to
  `:connection_reset`; timer closure maps to `:connection_timeout`.
- Connection keys are held in a bounded `BTreeMap`, allowing an inbound RST to
  classify one connection without scanning the socket table. The stable
  socket table remains the Phase 3 `BTreeMap`, including its cursor-resumable
  readiness overflow sweep.
- `sockname` is available after bind or connect initiation. `peername` is
  available while connecting or connected. The stable absence errors are
  `:not_bound` and `:not_connected`.
- Phase 4 close is abortive. It aborts the smoltcp socket, performs one bounded
  stack drive so any RST and current timer effects are returned, removes the
  native socket and public entry, and aborts each installed waiter exactly
  once with `:closed`. The public identity is invalid immediately, so a late
  SYN/ACK cannot revive it.
- Open, bind, names, and cancellation are non-driving operations and preserve
  the current timer. Connect, close, ingress, and timer poll return bounded
  output plus the authoritative `poll_at`, which replaces the BEAM timer.

The stable Phase 4 public error vocabulary is:

```text
unsupported_family unsupported_socket unsupported_timeout invalid_options
invalid_address invalid_port scope_required invalid_scope
address_in_use address_not_available ephemeral_ports_exhausted
network_unreachable connection_refused connection_reset connection_timeout
already_connected not_bound not_connected busy closed invalid_socket
invalid_socket_state system_limit
```

## Consequences

The public API now supports IPv6 TCP open, bind, nonblocking connect, endpoint
queries, cancellation, and abortive close without adding a socket process or a
native polling thread. TCP payload I/O, synchronous timeout wrappers, graceful
close, listen/accept, IPv4, and UDP remain later-phase work.

The small ephemeral subset makes exhaustion observable and testable while
remaining inside IANA's dynamic/private range. Port reuse by distinct local
addresses is intentionally unavailable until a later phase has evidence that
the extra tuple and listener interactions are required.
