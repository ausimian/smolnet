# ADR 0007: IPv6 TCP listener pooling

## Status

Accepted for Phase 7.

## Context

A `smoltcp` TCP socket stops listening when it receives a connection, while an
OTP listener must remain reusable across sequential and concurrent accepts.
Treating that connected socket as though it were still a listener would mix
identities, readiness, ownership, and lifecycle state. An unbounded pool or
accepted-child queue would violate the stack's scheduling and memory
invariants.

## Decision

- A public listener identity owns a native pool of
  `min(application_backlog, 4)` listening TCP sockets. Application backlogs are
  limited to `1..128` and default to 5 in the inet adapter. Pool members have
  no public identity and are never exposed as sockets. A half-open handshake
  expires after the same 30-second native timeout as an outbound connect; timer
  maintenance recycles the slot even when no further packet arrives.
- When a pool member reaches `Established` or `CloseWait`, native maintenance
  promotes it to a connected `TcpRecord`, allocates a fresh public socket ID
  and generation, appends that identity to the listener's FIFO accepted queue,
  and installs a new listening pool member. Accepted connections retain the
  listener's local port without blocking a later listener rebind.
- The accepted queue never exceeds the requested backlog. If it is full, or if
  the bounded public socket table cannot allocate a child identity, the newest
  established connection is removed and the pool slot is replenished. The
  remote may have observed handshake completion; later unmatched traffic
  causes TCP reset behavior. Existing queued children are never displaced.
- Listener scans use a stable cursor and the stack's existing
  `maintenance_work` budget. A one-unit budget alternates listener maintenance
  with normal egress work, so replenishment cannot create an unbounded NIF call
  or permanently starve established sockets. Snapshot metrics expose listener,
  pool, queue, backlog, promotion, refill, and overflow counts.
- `accept` uses the listener identity's read waiter slot with operation
  `:accept`. It performs an atomic bounded scan-and-arm, registers one-shot
  receive wakers on the pool, and uses the existing identity/reference select
  and cancellation protocol. Synchronous finite and infinite waits remain
  caller-side loops with one monotonic deadline.
- Closing a listener removes its public identity, aborts its pending accept,
  and releases its queued children plus listening and half-open pool members.
  Children already returned by `accept` are independent and remain usable.
  Stack shutdown drains listener and connected state together; stale or late
  readiness and packets cannot address a replacement identity.
- `SmolNet.InetBackend.Tcp` uses one adapter state for a listener and starts a
  separate temporary connected-stream adapter for every accepted child. The
  process calling `:gen_tcp.accept/2` initially owns that child. Supported
  active, mode, packet, packet-size, buffer, and send-timeout options are
  inherited as values, not shared mutable state. During adapter startup, the
  stack atomically monitors the listener adapter as the child's temporary
  owner; the child adapter validates the identity and atomically replaces that
  monitor when it adopts the socket, so an untrappable listener death cannot
  orphan a promoted native handle.

## Consequences

Low-level `SmolNet.listen/2` and `SmolNet.accept/1,2`, plus public
`:gen_tcp.listen/2` and `:gen_tcp.accept/1,2`, now provide complete IPv6 TCP
client/server operation without moving framing, active delivery, application
timeouts, or arbitrary queues into Rust.

The fixed pool cap trades very large kernel-style SYN backlogs for an explicit,
observable embedded-stack bound. IPv4 dispatch remains Phase 8 work. UDP,
additional packet modes, and operating-system file descriptor adoption remain
later or explicitly unsupported work.
