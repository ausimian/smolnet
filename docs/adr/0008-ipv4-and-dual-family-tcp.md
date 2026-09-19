# ADR 0008: IPv4 and dual-family TCP

## Status

Accepted for Phase 8.

## Context

IPv4 must reach parity only after the IPv6 TCP lifecycle is complete. Adding a
second address family must not let endpoint conversion, port reservations,
connection keys, readiness, or listener promotion cross family boundaries.
The raw-IP boundary must also validate IPv4-specific fields before smoltcp can
mutate stack state.

## Decision

- Enable smoltcp's `proto-ipv4` feature alongside `proto-ipv6`. One interface
  and `SocketSet` may hold IPv4 and IPv6 addresses, routes, sockets, and
  listeners simultaneously. Address configuration infers the family from a
  four-octet or eight-segment tuple; route destinations and gateways must use
  the same family.
- Raw IPv4 ingress requires a valid IHL, an exact total length, a valid header
  checksum, and a packet no larger than the configured MTU and work limit.
  SmolNet does not reassemble IPv4 fragments: a nonzero fragment offset or the
  more-fragments flag is rejected as `:invalid_packet`.
- Socket family is explicit and immutable from `open/4`. Public socket values
  retain `:inet` or `:inet6`, and both Elixir and Rust reject a bind or connect
  endpoint from the other family as `:invalid_address`. IPv4-mapped IPv6
  addresses are also rejected; SmolNet performs no implicit family conversion.
- Native endpoint and connection-key storage supports both address widths.
  Connection keys include a family discriminator, preventing an IPv4 address
  from colliding with an IPv6 byte pattern. Accepted sockets derive and retain
  their listener family.
- Bind and ephemeral-port conflicts are family-scoped. This permits an IPv4
  listener and an IPv6 listener on the same stack to use the same numeric port
  while retaining the existing no-reuse policy inside each family.
- A smoltcp wildcard listen endpoint has no family tag, so SmolNet never gives
  a dual-family wildcard directly to smoltcp. Each idle pool member starts on a
  configured address of its selected family and is retargeted to the incoming
  local destination before raw packet dispatch. At most the matching
  listener's four bounded pool members are considered. Half-open and connected
  members already have concrete tuples and are never retargeted. Public
  `sockname` continues to report the requested wildcard endpoint.
- `SmolNet.Inet6.Tcp` is the IPv6 OTP callback and the shared adapter process
  implementation. `SmolNet.Inet.Tcp` is a thin
  IPv4 callback that delegates address parsing to OTP's `:inet_tcp`, selects
  `:inet`, and returns sockets backed by the shared adapter. Framing, active
  mode, ownership, deadlines, and lifecycle policy therefore do not fork.
- The deterministic raw-IP link harness is protocol-neutral. Dual-family tests
  drive both packet versions through one pair of stacks and verify their link
  references, endpoint names, port isolation, and stream contents.
- IPv4 limited broadcast is not a valid interface address, route gateway, or
  TCP endpoint. Both Elixir and native validation reject it before smoltcp.

## Consequences

IPv4 and IPv6 TCP clients and servers now share the same bounded scheduling,
readiness, listener-pool, and adapter invariants. UDP remains unavailable; this
decision completes the TCP protocol-family gate without introducing NAT64,
IPv4-mapped endpoints, IPv4 reassembly, or dual-stack wildcard semantics.
