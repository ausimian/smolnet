# Path MTU

A stack's `:mtu` is the largest IP packet it sends or accepts. The path to
a peer can be narrower, with a tunnel, a PPPoE link or a VPN in the way.
This guide says what SmolNet does then, and how to configure a link so
that the difference never matters.

## What SmolNet does

- Every TCP SYN it sends advertises an MSS of `:mtu` less 40 bytes for
  IPv4, or less 60 for IPv6. A connection's segments start as the smaller
  of that and the peer's MSS.
- Every IPv4 packet it sends has Don't Fragment set. It fragments nothing,
  in either family.
- It does path MTU discovery for TCP from ICMP errors (RFC 1191, RFC
  8201); see [below](#path-mtu-discovery).
- It does **not** detect a black hole: when a router's ICMP errors are
  dropped on the way back, nothing tells SmolNet that its segments are
  too big. RFC 4821 and RFC 8899 are unimplemented.
- It does **not** reassemble fragments. The ingress check refuses an IPv4
  fragment as `:invalid_packet`, and an IPv6 fragment is dropped; see
  [#129](https://github.com/ausimian/smolnet/issues/129).
- `:mtu` may be from 1280 to 65,575, so no `:mtu` fits an IPv4 path
  narrower than 1280.

## Path MTU discovery

A router that cannot forward a TCP segment because it is too big for the
next hop drops it, and sends back ICMP "Fragmentation Needed" or ICMPv6
"Packet Too Big" with that hop's MTU. When one reaches SmolNet:

- The connection that sent the segment lowers its segment size to fit
  the reported MTU, and at once sends everything unacknowledged again, in
  segments of the new size. It does not wait for a retransmission timeout,
  and its congestion window is not cut: the drop was not congestion.
- The size only ever falls. It never goes below 536 bytes for IPv4, a
  path MTU of 576, or 1220 for IPv6, the IPv6 minimum of 1280. SmolNet
  sets Don't Fragment on every packet, so an IPv4 path narrower than 576
  stays a black hole; clamp the MSS for one of those.
- An error from a router that predates RFC 1191, without an MTU in it,
  counts as the next plateau of RFC 1191 below the packet it refused:
  1492 for a 1500-byte packet, then 1006.
- The error must quote a segment of that connection that is still
  unacknowledged, as RFC 5927 recommends. One forged without seeing the
  connection, which would have to guess its sequence numbers, is ignored,
  as is one whose MTU is not below the packet it quotes.
- What a connection learns is its own. SmolNet keeps no cache of path
  MTUs by destination, so each new connection starts at the full size
  again, loses its first full-size segments to the same hop, and learns
  the MTU from the error, one round trip to the router later. The MSS in
  its SYN comes from `:mtu` alone.
- UDP is not covered: a datagram too big for the path is lost, and the
  error changes nothing; see
  [#129](https://github.com/ausimian/smolnet/issues/129).

`SmolNet.stack_info/1` counts these errors, in the native snapshot's
`counters`:

- `icmp_too_big_received`: the "Fragmentation Needed" and "Packet Too
  Big" errors about a TCP segment the stack received.
- `icmp_too_big_rejected`: those it ignored as invalid, because they were
  malformed, quoted a packet it did not send or a connection it does not
  have, or quoted data not in flight.
- `path_mtu_reductions`: the times a connection's segment size fell.

## What happens on a narrower path

`integration/pmtu.exs` measures it. SmolNet and a Linux peer are at 1500,
with a hop between them at 1280, 1000 or 576 for IPv4, and at 1280 for
IPv6:

| traffic | the hop's router sends ICMP errors | it drops them | the router clamps the MSS |
| --- | --- | --- | --- |
| TCP, SmolNet sending | adapts | stalls | adapts |
| TCP, the peer sending | adapts | stalls | adapts |
| TCP, the peer sending without Don't Fragment (IPv4) | stalls | stalls | adapts |
| UDP datagrams larger than the path, either way | lost | lost | lost |

A peer that sends without Don't Fragment gets no ICMP error at all: the
router fragments its packets instead, and SmolNet discards the fragments.

Where the errors are dropped, TCP stalls in the sender's direction, and
the stall is silent:

- The connection is established, and a send returns `:ok`.
- The peer receives nothing.
- SmolNet keeps resending the first segment, backing off each time, and
  never returns an error.

Anything that fits in one packet still works, such as a handshake, a short
request or a small reply. So the fault shows only once something large is
sent: a TLS handshake with a long certificate chain, an upload, or a big
query.

Either end adapts only because it acts on the router's ICMP error.
Filter that error anywhere on the path, and the sender stalls, whichever
end it is. Linux's `net.ipv4.tcp_mtu_probing=1` would let a kernel peer
recover even then, but it is off by default, and SmolNet has no
equivalent.

## Configuring a link

With the path's ICMP errors reaching SmolNet, TCP needs no configuration.
What follows is for paths that filter them, which are black holes, and
for UDP, which path MTU discovery does not cover.

**Set `:mtu` to the narrowest MTU on the path, when that is 1280 or
more.** This is the fix to reach for first:

- SmolNet then advertises an MSS that fits, and never sends a larger
  packet, so TCP works in both directions with no help from the network,
  and no connection loses its first segments finding out.
- For a tunnel, use the tunnel's MTU, not the MTU of the link beneath it.
- Give the device or transport that carries the stack's packets the same
  MTU.

```elixir
# A WireGuard tunnel, whose interface MTU is 1420
SmolNet.start_stack(egress: {self(), :wg}, mtu: 1420, addresses: addresses)
```

**Otherwise, clamp the MSS on the router at the narrow link.** This covers
an IPv4 path below 1280, which no `:mtu` fits, and a stack whose `:mtu`
you cannot change. With nftables, in the forward chain of the router that
owns the narrow link:

```
tcp flags & (syn | rst) == syn tcp option maxseg size set rt mtu
```

or, with iptables, `-p tcp --tcp-flags SYN,RST SYN -j TCPMSS
--clamp-mss-to-pmtu`. Both lower a SYN's MSS to the smaller of the route's
MTU each way, so they cover SmolNet's SYNs and the peer's alike. The
scenario shows that the nftables rule is enough, with or without ICMP.

The clamp only knows its own router's links. It cannot see a narrower hop
further along, so a path that narrows further out, on the internet, still
stalls if that hop's errors are filtered. If you know the path's MTU but
the router's own links are wider,
clamp to a fixed MSS instead: for a path of 1280, `set 1240` for IPv4
SYNs (`meta nfproto ipv4`) and `set 1220` for IPv6.

**Let ICMP errors through, both ways.** Do not filter ICMP type 3 code 4
or ICMPv6 type 2 anywhere you control. SmolNet's own sends depend on
them, as do a kernel peer's.

**Keep UDP datagrams within the path.** SmolNet's own limit, `:mtu` less
28 bytes for IPv4 or 48 for IPv6, is not the path's. A larger datagram is
lost, and so is one that arrives in fragments. For DNS, advertise an EDNS
UDP size of 1232 or less, as DNS Flag Day 2020 recommends.

| path MTU | TCP MSS, IPv4 | TCP MSS, IPv6 | largest UDP payload, IPv4 | largest UDP payload, IPv6 |
| --- | --- | --- | --- | --- |
| 1500, Ethernet | 1460 | 1440 | 1472 | 1452 |
| 1492, PPPoE | 1452 | 1432 | 1464 | 1444 |
| 1420, WireGuard | 1380 | 1360 | 1392 | 1372 |
| 1280, the IPv6 minimum | 1240 | 1220 | 1252 | 1232 |
| 576 | 536 | - | 548 | - |

## Recognising a black hole

The signs:

- Connections open, and small exchanges succeed.
- Larger transfers hang with no error, in one direction or both.

In a capture on SmolNet's side of the link, the sign is SmolNet resending
the same full-size segment, backing off each time, with nothing from the
peer and no error from a router. With errors arriving, a capture shows
each first full-size flight answered by "need to frag (mtu N)" or "packet
too big, mtu N", and SmolNet resending at once in smaller segments.

In `SmolNet.stack_info/1`, a black hole shows as `icmp_too_big_received`
staying at 0 while sends hang. A rising `icmp_too_big_rejected` means
errors arrive but do not match what SmolNet sent: a NAT that rewrites the
packets but not the headers the errors quote, or forged errors.

To see how a path of your own behaves, run the scenario in a network
namespace of its own. It needs no sudo where the host allows unprivileged
user namespaces:

```console
integration/pmtu-topology.sh isolate mix run integration/pmtu.exs
```

See `integration/README.md` in the source repository.
