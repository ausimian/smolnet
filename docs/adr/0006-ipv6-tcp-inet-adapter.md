# ADR 0006: IPv6 TCP inet adapter

## Status

Accepted for Phase 6.

## Context

OTP applications expect TCP clients to use `:gen_tcp` and `:inet` socket
operations, ownership, active delivery, and framing. Those policies must remain
outside Rust and the serialized stack owner. The adapter also has to work with
the OTP 27, 28, and 29 TCP callback contract without relying on a globally
registered stack or sharing mutable state between logical sockets.

## Decision

- `SmolNet.InetBackend.Tcp` is both the thin OTP callback module and a
  per-socket `:gen_statem`. OTP socket terms have the callback form
  `{'$inet', module, pid}`. Each adapter is a temporary child of its stack's
  anonymous inet-backend supervisor and starts bounded open/connect work only
  after `init/1` returns.
- A connection must include `{:smolnet_stack, stack}` and an IPv6 peer. IPv4 is
  `:eafnosupport`; listen, accept, and file-descriptor adoption are `:enotsup`
  in this phase. Address parsing and service lookup delegate to OTP's IPv6 TCP
  module, while operations on an established socket are process calls.
- One adapter owns all application policy: controlling-process state, passive
  deadlines, active mode, binary/list representation, framing, bounded receive
  buffering, and unsent write data. Rust and `SmolNet.Stack` retain none of
  these values.
- One read continuation and one write continuation may coexist. A competing
  operation in the same direction returns `:busy`; changing framing or receive
  bounds while a passive receive is pending also returns `:busy`. Native select
  messages remain one-shot retry hints matched by socket identity and reference.
- Packet modes are `:raw`, `:line`, `1`, `2`, and `4`. Prefixes are unsigned
  big-endian lengths. Framing spans arbitrary native chunks and active counts
  measure complete logical packets. `packet_size` and the receive buffer
  default to 65,536 bytes and are capped at 1 MiB. An oversized inbound frame
  is terminal because its stream boundary cannot be recovered safely.
- Active mode is `false`, `true`, `:once`, or `N` in `1..32_767`. `:once`
  becomes passive after one packet; counted mode sends `{:tcp_passive, socket}`
  when its count reaches zero. A mailbox turn performs at most 16 native reads
  or 16 logical deliveries before scheduling a continuation.
- Send and receive timeouts are absolute monotonic deadlines owned by the
  adapter. Timeout cancels the exact native select registration. A partially
  accepted send retains only its remaining sub-binary while retrying and
  returns that remainder with a later timeout or error, matching OTP's
  non-buffering socket-style contract. `send_timeout_close` closes the adapter
  after a send timeout.
- Controlling-process transfer first pauses active reads, then the old owner
  removes matching queued TCP messages. The adapter commits the new monitored
  owner and sends those messages before resuming reads. If commit fails, the
  old owner restores the removed messages. This prevents old and new active
  data from crossing during handoff.
- Every adapter monitors its controlling process and stack server. The stack
  server additionally monitors the adapter that owns each low-level socket,
  allowing it to close the native socket even after an untrappable adapter
  kill. Owner death, adapter termination, stack failure, and bundle shutdown
  are idempotent; adapters are never restarted, and independent stacks remain
  isolated.

## Consequences

Existing OTP client code can use SmolNet by selecting the custom TCP module and
providing a stack reference. Active delivery, packet parsing, ownership, and
timeouts run in independent BEAM socket processes, so the native stack owner
continues to perform only serialized bounded work.

The adapter exposes zero-valued placeholder statistics for the small OTP
`getstat` callback surface; native traffic counters are not yet tracked.
Listening and accepting, IPv4, additional packet modes, UDP, and operating-
system file descriptors remain explicit later-phase work.
