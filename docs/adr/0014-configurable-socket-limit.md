# ADR 0014: configurable socket limit

## Status

Accepted. Supersedes the socket-entry and waiter caps in ADR 0003.

## Context

A stack held at most 64 native backing sockets, a constant rather than a stack
option. A TCP socket that closes first keeps its slot through TIME-WAIT, about
10 s, so a stack whose sockets close first could open only about six new
connections per second before opens failed with `:system_limit` (#56). Short
connections are common: HTTP clients without keep-alive, health checks, and
pools that recycle connections all close first.

Two further bounds depended on `ready_events`, the per-call readiness budget,
whose maximum is 128 (ADR 0003). The socket table admitted fewer live entries
than `ready_events`, counting TCP sockets still closing, and it capped
outstanding waiters at `ready_events`. The defaults happened to line up: 64
sockets with one read and one write waiter each is 128 waiters. Raising only the
native capacity would still have failed the 129th waiter, and the 128th open,
with `:system_limit`.

Those caps were there to bound table traversal, orderly shutdown, and resource
destruction. Traversal and shutdown have since become incremental: the
readiness sweep keeps a cursor and scans at most one readiness budget of
entries per call, and shutdown drains its structures and aborts across bounded
continuations. Only unexpected resource destruction still does all its work in
one synchronous call, so it is what the maximum must fit (ADR 0011).

## Decision

- `limits: %{sockets: n}` sets how many native backing sockets a stack may
  hold. It defaults to 64, so existing stacks keep their capacity, and may be
  raised to 512. Elixir and the NIF validate it with the other limits.
- The same value bounds the socket table: live entries plus TCP sockets still
  closing. For real sockets this bound can never bind before the native one,
  because every logical socket owns at least one native socket. It remains as a
  defensive bound, and it also bounds the debug-only synthetic sockets.
- The waiter cap is twice `sockets`, since a socket holds at most one waiter in
  each direction. A public operation can no longer exhaust it. A debug-only
  hook lowers it so tests still cover the refusal path.
- `ready_events` is now only a per-call work budget. Lowering it no longer
  lowers how many sockets or waiters a stack can hold.
- A stack's socket buffers total at most 128 MiB, what 64 sockets with the
  largest TCP buffers (1 MiB each way) could hold under the old fixed limit.
  An open, a listener's pool expansion or refill, or a wildcard UDP bind that
  would pass it returns `:system_limit`, and `stack_info/1` reports the total
  as `socket_buffer_bytes`. Without it, 512 sockets with the largest buffers
  would hold 1 GiB, and an unexpected resource drop would free all of it in
  one synchronous destructor. Freeing memory the sockets have written to is
  proportional to its size, and on Linux `munmap` returns those pages
  synchronously. The cap keeps that worst case where it already was. The
  total is recomputed from the socket records when a socket opens, the same
  way the native socket count is, rather than tracked alongside every
  allocation.
- The shutdown continuation guard in `SmolNet.Stack` grows by 16 calls per
  socket, since each socket adds a few structures to drain and up to two aborts
  to deliver.
- TIME-WAIT sockets count against the same limit as open ones. Freeing their
  buffers early would mean replacing smoltcp's socket in place and is left for
  later. `stack_info/1` already reports them separately as
  `closing_tcp_socket_count`.

## Verification

`scripts/nif_budget.exs` runs every maximum-state scenario at 512 sockets:
512 closing sockets, 512 sockets with both waiters armed, 512 UDP sockets, and
64 wildcard UDP sockets across eight addresses, each with a retained sent read
waiter and an active write waiter. On an Apple silicon Mac with the debug NIF,
the slowest scenario was destroying the resource holding 512 UDP sockets and
1,024 saved waiter terms: p99 353 µs, maximum 505 µs over 100 samples.
Closing-socket maintenance took at most 171 µs, and the combined output,
cleanup, and readiness call at most 290 µs. Destroying a stack at the buffer
cap, 64 TCP sockets with 1 MiB buffers each way plus waiters on all 512
slots, took p99 174 µs, and its orderly shutdown p99 170 µs per call. Those
buffers were never written to; freeing 1 GiB of written 1 MiB buffers took
288 µs in a separate macOS measurement, and would take longer on Linux,
which is why the cap stays at the pre-existing 128 MiB rather than growing
with the socket limit.

At 1,024 sockets the same destructor took 1.03 ms and the combined call
1.32 ms, both over the 1 ms normal-scheduler target, which is why the maximum is
512.

## Consequences

A stack whose sockets close first can sustain about `sockets / 10` new
connections per second, about 51 at the maximum. Applications that need more
can run more stacks, or keep connections open.

Each slot holds its buffers from open until the slot is freed: 64 KiB each way
for a TCP socket by default and up to 1 MiB each way, and 32 KiB for a UDP
socket. A raised limit is a deliberate memory choice; at default buffer sizes,
512 TCP sockets hold about 64 MiB. The 128 MiB buffer cap means a stack cannot
have both the most sockets and the largest buffers: 512 sockets must average
at most 256 KiB of buffer each.

Several per-call smoltcp operations, such as egress polling and ingress socket
lookup, visit every socket in the stack. They stay under the call deadline at
512 sockets, but a stack with many sockets does more work per call than one
with few.
