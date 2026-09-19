# Using `:gen_udp`

SmolNet can back Erlang's `:gen_udp` and `:inet` APIs. Use this interface for
OTP-style datagram sockets, including passive receives, active messages,
connected UDP, and controlling-process ownership.

## Select the SmolNet backend

Every socket needs two SmolNet-specific options:

- `{:udp_module, module}` selects the callback for its address family.
- `{:smolnet_stack, stack}` selects the independent stack that owns it.

Use matching callback and family options:

| Family | Callback | Family option |
| --- | --- | --- |
| IPv4 | `SmolNet.Inet.Udp` | `:inet` |
| IPv6 | `SmolNet.Inet6.Udp` | `:inet6` |

For example, these are passive binary IPv6 options:

```elixir
options = [
  {:udp_module, SmolNet.Inet6.Udp},
  {:smolnet_stack, stack},
  :inet6,
  :binary,
  {:active, false}
]
```

The callback option affects only sockets created with that option list. It does
not replace the node-wide inet backend.

## A complete loopback example

This example opens two IPv4 UDP sockets on one looped stack and sends a
datagram between them:

```elixir
{:ok, _link, stack} =
  SmolNet.Loopback.start_link(addresses: [{{127, 0, 0, 1}, 8}])

options = [
  {:udp_module, SmolNet.Inet.Udp},
  {:smolnet_stack, stack},
  :inet,
  :binary,
  {:active, false},
  {:ip, {127, 0, 0, 1}}
]

{:ok, receiver} = :gen_udp.open(0, options)
{:ok, {{127, 0, 0, 1}, receiver_port}} = :inet.sockname(receiver)

{:ok, sender} = :gen_udp.open(0, options)
:ok = :gen_udp.send(sender, {127, 0, 0, 1}, receiver_port, "ping")

{:ok, {{127, 0, 0, 1}, _sender_port, "ping"}} =
  :gen_udp.recv(receiver, 0, 5_000)

:ok = :gen_udp.close(sender)
:ok = :gen_udp.close(receiver)
:ok = SmolNet.stop_stack(stack)
```

Port zero chooses an ephemeral port from the stack's bounded range. UDP and TCP
have separate port namespaces, as do IPv4 and IPv6.

## Sending and receiving

Open, send, and receive with the normal `:gen_udp` API:

```elixir
{:ok, socket} = :gen_udp.open(0, options)
:ok = :gen_udp.send(socket, peer_address, 53, "query")

{:ok, {source_address, source_port, response}} =
  :gen_udp.recv(socket, 0, 5_000)
```

Each send accepts one complete datagram or fails without accepting any of it.
The payload limit is the smaller of 16,384 bytes and the configured MTU minus
the IP and UDP headers. An oversized datagram returns `:emsgsize`.

`recv(socket, 0, timeout)` returns the complete next datagram, including a
zero-length datagram. A positive receive length truncates a larger datagram and
discards the remainder. The `:gen_udp` result does not separately report that
truncation, matching the callback contract; use the low-level `SmolNet` API if
the application needs a `truncated` flag or the packet's local destination.

## Connected UDP

Connecting records a default peer; it does not perform a handshake:

```elixir
:ok = :gen_udp.connect(socket, peer_address, 53)
:ok = :gen_udp.send(socket, "connected query")
```

After connection, sends must target that peer and incoming datagrams from other
peers are discarded. `:inet.peername/1` returns the configured peer.

## Active delivery

Active mode supports `true`, `:once`, and counts from 1 through 32,767:

```elixir
:ok = :inet.setopts(socket, active: :once)

receive do
  {:udp, ^socket, source_address, source_port, packet} ->
    handle_datagram(source_address, source_port, packet)

  {:udp_error, ^socket, reason} ->
    handle_error(reason)
end
```

Counted mode sends `{:udp_passive, socket}` when its count is exhausted. Each
adapter mailbox turn delivers at most 16 datagrams before yielding. Switch back
to passive mode with `:inet.setopts(socket, active: false)` before calling
`:gen_udp.recv/3`.

Only the controlling process receives active messages. Transfer ownership with
`:gen_udp.controlling_process/2`; queued matching messages and future delivery
move to the new owner in order. If the owner exits, the adapter and its
low-level socket close.

## Buffers and wildcard binds

`buffer` and its `recbuf` alias bound the payload returned by passive and active
receives. They default to 65,536 bytes and accept values from 1 byte through
1 MiB. If a received datagram is larger, the delivered payload is truncated
and the remainder is discarded.

A wildcard bind creates one family-specific native backing socket for each
configured local address in that family. Those backing sockets share one
logical OTP socket, receive queue, and controlling process. This preserves the
actual destination address internally and lets the stack choose a source
address for outbound traffic.

## Supported options

The UDP option surface is deliberately finite:

| Option | At open | At runtime |
| --- | --- | --- |
| `:smolnet_stack` | required | fixed |
| `:inet` / `:inet6` | supported | fixed |
| `:binary` / `:list` / `:mode` | supported | supported |
| `:active` | `false`, `true`, `:once`, or `1..32767` | supported |
| `:buffer` / `:recbuf` | `1..1048576` | supported |
| `:ip` / `:ifaddr` / `:port` | supported | fixed |
| `:ipv6_v6only` | IPv6 `true` only | fixed |

Invalid values, options used in the wrong lifecycle phase, and unlisted
options return `:einval`. Selecting the wrong family returns `:eafnosupport`.
Packet framing, send timeouts, ancillary data, multicast, broadcast, OS file
descriptors, and raw socket options are not supported.

Network and lifecycle errors use OTP-style atoms such as `:enetunreach`,
`:eaddrinuse`, `:emsgsize`, `:closed`, and `:enetdown`. One read and one write
may progress independently; competing operations in the same direction return
`:busy`.
