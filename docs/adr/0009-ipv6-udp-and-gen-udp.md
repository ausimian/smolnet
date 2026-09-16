# ADR 0009: Bounded IPv6 UDP and `gen_udp`

## Status

Accepted for Phase 9.

## Context

UDP must preserve whole datagrams and endpoint metadata while reusing the
bounded ownership, readiness, timer, output, and lifecycle paths already proven
by TCP. Stream-style partial progress would make retries ambiguous and could
duplicate datagrams. OTP active-mode and ownership policy also belongs in
Elixir rather than the native stack.

## Decision

- Enable smoltcp's UDP socket feature and store UDP sockets in the shared
  `SocketSet` and stable public socket table. Every native UDP socket owns fixed
  receive and transmit packet buffers: 16 metadata entries and 16 KiB of
  payload in each direction. UDP and TCP bind conflicts and ephemeral cursors
  are independent, while both protocols share the global public socket limit.
- Phase 9 opens only `:inet6` UDP sockets. Endpoint conversion keeps family and
  scope validation explicit; IPv4 UDP returns `:unsupported_family` and is
  deferred to Phase 10.
- A datagram payload may not exceed `min(mtu - 48, 16_384)` bytes. Native send
  copies the complete payload into smoltcp or returns
  `:message_too_large`, `:network_unreachable`, or a write-direction select
  hint. It never reports or retains application-visible partial progress.
- Receive dequeues exactly one datagram. Length zero returns it whole; a
  positive length copies the prefix, discards the remainder, and returns a
  truncation flag. The result carries both the source endpoint and the actual
  local destination from packet metadata. Zero-length datagrams are valid.
  smoltcp validates the mandatory IPv6 UDP checksum and silently discards bad
  packets before they enter the receive ring.
- UDP installs read and write waiters in the existing socket table and uses the
  same ready queue, bounded sweep, one-shot notification, exact cancellation,
  deadline, output, and timer paths as TCP. There is no UDP-specific readiness
  queue. Close removes the socket immediately and aborts installed waiters.
- UDP connect stores a peer after bind and route validation. Connected sends
  must use that peer, `peername` reports it, and receive discards datagrams from
  other sources with a scan bounded by the 16-entry packet ring.
- `SmolNet.InetBackend.Udp` is both the thin IPv6 OTP callback and the
  `:gen_statem` implementation for each socket. `init/1` installs only local
  state and monitors; open, owner registration, and bind begin in an internal
  event. Each adapter is a temporary child of its stack's anonymous inet
  supervisor.
- The adapter holds independent read and write continuations. Active delivery
  is limited to 16 datagrams per mailbox turn, and supports `true`, `:once`, and
  counted modes with standard `udp` and `udp_passive` messages. Ownership
  transfer pauses receive draining while moving queued messages. Owner death,
  adapter death, and stack failure close or fail only the affected lifecycle.
- The supported option surface is deliberately small: binary/list mode,
  active mode, and bounded buffer/recbuf values. The adapter buffer caps each
  delivered payload and silently discards a truncated datagram remainder in
  the standard UDP callback shape. Packet framing, send timeout,
  ancillary data, multicast, broadcast, file descriptors, and other datagram
  options return explicit unsupported or invalid errors.

## Consequences

IPv6 UDP now coexists with IPv4 and IPv6 TCP in one stack without protocol,
identity, readiness, or port cross-talk. Public retries remain unambiguous and
bounded, and OTP policy remains outside Rust. IPv4 UDP and the complete
four-protocol matrix remain Phase 10 work.
