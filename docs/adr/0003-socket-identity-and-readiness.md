# ADR 0003: socket identity and readiness

## Status

Accepted for Phase 3.

## Context

smoltcp socket handles are internal collection indexes and may be reused after
a socket closes. They therefore cannot safely identify a public socket or a
delayed readiness message. Nonblocking operations also need to install a
waiter without leaving a lost-wakeup window between observing that an
operation would block and arming its waker.

Every native call must remain bounded, and no mutex acquisition on a normal
BEAM scheduler may wait for another thread.

## Decision

- Allocate process-global, monotonically increasing socket IDs and generations.
  Neither value wraps or is reused. Both are capped at 2^59 - 1, the largest
  positive 64-bit BEAM small integer, so encoding an identity never allocates a
  heap bignum. Exhaustion returns :system_limit.
- Represent a public socket as a SmolNet.Socket struct containing the stack PID,
  ID, and generation. Native table lookup validates ID, generation, kind, and
  lifecycle before touching socket state.
- Permit one read-direction and one write-direction waiter per socket. A
  competing waiter in the same direction returns :busy; the native table
  globally caps outstanding waiters at the configured per-call :ready_events
  limit so orderly shutdown can abort all waiters in one bounded call.
- Cap live socket entries at :ready_events as well. Opening beyond that limit
  returns :system_limit, making table traversal and orderly shutdown bounded;
  closing a socket releases one entry of capacity.
- Retain only the recipient PID, operation class, socket identity, and select
  reference. Application payloads, receive lengths, partial data, and
  deadlines remain in Elixir.
- Use smoltcp one-shot wakers solely to set a per-direction atomic ready flag
  and attempt to add a coalesced key to a bounded ready set. The callback never
  constructs BEAM terms or sends messages. Every internal lock uses try_lock;
  contention sets the overflow marker and relies on the atomic flag.
- Drain at most :ready_events units in the NIF epilogue. If the bounded ready
  set overflows, scan the socket table incrementally and schedule another stack
  continuation when work remains. This guarantees eventual notification
  without allowing the ready set or a single NIF call to grow without bound.
- Send one-shot notifications as
  {:"$smol_socket", {id, generation}, :select, reference}. Close and orderly
  stack shutdown use
  {:"$smol_socket", {id, generation}, :abort, reference, :closed}.
- Define cancellation outcomes as :ok when the exact waiter is removed,
  :already_sent when its notification already won the race, and :not_found for
  a nonmatching reference. Cancellation clears the old ready flag so a queued
  stale key cannot notify a later operation.
- Linearize stack shutdown before draining waiters and reject every later
  socket operation with :closed. Non-driving socket calls preserve any
  existing protocol poll deadline.

The Phase 3 synthetic readiness driver is compiled only into debug NIF builds
and is reached through test-support modules. It uses the same table, waker,
epilogue, cancellation, close, and shutdown paths without exposing a transport
protocol before Phase 4.

## Consequences

Readiness messages are retry hints, not completion promises. Repeated and
spurious wakes are harmless, and a caller must retry and may receive a new
select reference. Late queue entries are rejected by ID/generation and exact
reference matching.

The smoltcp async feature is now required, but SmolNet still has no native
thread or executor. The finite identity space is intentionally not recycled;
exhausting roughly 576 quadrillion identities is a stable terminal allocation
error rather than a reason to weaken stale-event safety.
