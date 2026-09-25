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
  sockets can hold about 64 MiB.

### Changed

- The `ready_events` limit no longer caps how many sockets and blocked
  operations a stack holds. It used to admit fewer sockets than its value and
  at most that many waiting operations, so lowering it could make opens, and
  `:nowait` sends, receives, accepts and connects, fail with `:system_limit`.
  It now bounds only how many readiness events one native call delivers, and
  `sockets` governs capacity.
