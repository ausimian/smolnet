# Path MTU

A stack's `:mtu` is the largest IP packet it sends or accepts. The path to
a peer can be narrower, with a tunnel, a PPPoE link or a VPN in the way.
This guide says what SmolNet does then, and how to configure a link so
that the difference never matters.

## What SmolNet does

- Every TCP SYN it sends advertises an MSS of `:mtu` less 40 bytes for
  IPv4, or less 60 for IPv6. A connection's segments are the smaller of
  that and the peer's MSS, for the whole connection.
- Every IPv4 packet it sends has Don't Fragment set. It fragments nothing,
  in either family.
- It does **not** do path MTU discovery. It ignores ICMP "Fragmentation
  Needed" and ICMPv6 "Packet Too Big", and nothing detects a black hole.
  RFC 1191, RFC 8201 and RFC 4821 are unimplemented; see [#128](https://github.com/ausimian/smolnet/issues/128).
- It does **not** reassemble fragments. The ingress check refuses an IPv4
  fragment as `:invalid_packet`, and an IPv6 fragment is dropped; see
  [#129](https://github.com/ausimian/smolnet/issues/129).
- `:mtu` may be from 1280 to 65,575, so no `:mtu` fits an IPv4 path
  narrower than 1280.

## What happens on a narrower path

`integration/pmtu.exs` measures it. SmolNet and a Linux peer are at 1500,
with a hop between them at 1280, 1000 or 576 for IPv4, and at 1280 for
IPv6:

| traffic | the hop's router sends ICMP errors | it drops them | the router clamps the MSS |
| --- | --- | --- | --- |
| TCP, SmolNet sending | stalls | stalls | adapts |
| TCP, the peer sending | adapts | stalls | adapts |
| TCP, the peer sending without Don't Fragment (IPv4) | stalls | stalls | adapts |
| UDP datagrams larger than the path, either way | lost | lost | lost |

A peer that sends without Don't Fragment gets no ICMP error at all: the
router fragments its packets instead, and SmolNet discards the fragments.

A stall is silent:

- The connection is established, and a send returns `:ok`.
- The peer receives nothing.
- SmolNet keeps resending the first segment, backing off each time, and
  never returns an error.

Anything that fits in one packet still works, such as a handshake, a short
request or a small reply. So the fault shows only once something large is
sent: a TLS handshake with a long certificate chain, an upload, or a big
query.

When the peer sends, it adapts only because its own kernel acts on the
router's ICMP error. Filter that error anywhere on the path, and the peer
stalls too. Linux's `net.ipv4.tcp_mtu_probing=1` would let the peer
recover even then, but it is off by default.

## Configuring a link

**Set `:mtu` to the narrowest MTU on the path, when that is 1280 or
more.** This is the fix to reach for first:

- SmolNet then advertises an MSS that fits, and never sends a larger
  packet, so TCP works in both directions with no help from the network.
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
stalls until [#128](https://github.com/ausimian/smolnet/issues/128) is
fixed. If you know the path's MTU but the router's own links are wider,
clamp to a fixed MSS instead: for a path of 1280, `set 1240` for IPv4
SYNs (`meta nfproto ipv4`) and `set 1220` for IPv6.

**Let ICMP errors through to the peer.** Do not filter ICMP type 3 code 4
or ICMPv6 type 2 anywhere you control. SmolNet ignores them, but a
kernel peer sending to SmolNet depends on them.

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
the same full-size segment, each copy answered by "need to frag (mtu N)"
or "packet too big, mtu N" from a router. If no error comes back, there
are just the resends, and nothing from the peer.

To see how a path of your own behaves, run the scenario in a network
namespace of its own. It needs no sudo where the host allows unprivileged
user namespaces:

```console
integration/pmtu-topology.sh isolate mix run integration/pmtu.exs
```

See `integration/README.md` in the source repository.
