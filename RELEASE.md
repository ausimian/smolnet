### Changed

- A stack checks the header checksum of each IPv4 packet it receives about
  seven times faster, cutting the work its process does per IPv4 packet by
  about 40%. Bulk TCP over IPv4 on a loopback link runs about 10% faster,
  level with IPv6. A packet with a bad checksum is still refused with
  `{:error, :invalid_packet}`.
