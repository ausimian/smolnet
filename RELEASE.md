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
