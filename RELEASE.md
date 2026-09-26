### Changed

- A stack checks the header checksum of each IPv4 packet it receives about
  seven times faster, cutting the work its process does per IPv4 packet by
  about 40%. Bulk TCP over IPv4 on a loopback link runs about 10% faster,
  level with IPv6. A packet with a bad checksum is still refused with
  `{:error, :invalid_packet}`.
- Received TCP data is copied once on its way from a stack to the socket
  owner, instead of two or three times. A 64 KiB `:gen_tcp.recv/2` of data
  that is already queued returns about three times faster, and bulk
  transfers that read large chunks, with `recv(0)` or in active mode, run
  about 20% faster on a loopback link.
