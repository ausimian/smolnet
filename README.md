# SmolNet

SmolNet is an OTP-friendly, userspace TCP/IP stack for Elixir. It embeds
[`smoltcp`](https://github.com/smoltcp-rs/smoltcp) behind a deliberately small
Rustler NIF and exposes TCP and UDP through familiar `:gen_tcp`, `:gen_udp`,
and low-level socket-style APIs.

Each SmolNet stack is an independent network namespace. It exchanges complete
IPv4 and IPv6 packets with a link process supplied by the application, rather
than opening host operating-system sockets itself.

## Why a userspace network stack?

BEAM applications normally use the host kernel's network stack, which is the
right choice for ordinary clients and servers. An application-owned stack is
useful when packets need to travel over something other than a host network
interface, or when a program needs isolated addresses, routes, sockets, and
failure domains. Examples include encrypted tunnels, network simulations,
packet-level tests, and application-defined transports.

[gVisor's Netstack](https://gvisor.dev/docs/architecture_guide/networking/) is
a larger example of the same architectural idea: its Go userspace network
stack lets a sandbox handle network protocols without giving the application
direct access to the host network stack. SmolNet applies that idea at a smaller
scale for Elixir. It combines `smoltcp`'s protocol implementation with OTP
supervision, bounded native work, and standard Erlang socket interfaces.

SmolNet is transport-neutral. It does not provide an Ethernet device, TUN/TAP
setup, or a VPN protocol. Your link process decides where emitted raw IP
packets go and feeds packets from that transport back into the stack.

## Installation

Add `smolnet` to your dependencies:

```elixir
def deps do
  [
    {:smolnet, "~> 0.2"}
  ]
end
```

Hex packages include precompiled NIFs for glibc-based GNU/Linux on x86-64 and
AArch64, and Apple Silicon macOS 14 or newer. Other targets can build from a
source checkout with Rust 1.91 or newer.

## Starting a stack

Configure the addresses owned by the stack and identify the process that will
carry its outbound packets:

```elixir
local_address = {0xFD00, 0, 0, 0, 0, 0, 0, 1}

{:ok, stack} =
  SmolNet.start_stack(
    egress: {self(), :my_link},
    mtu: 1280,
    addresses: [{local_address, 64}]
  )
```

The link process receives bounded, ordered batches of complete raw IP packets:

```elixir
receive do
  {:smol_stack, :my_link, :egress, packets} ->
    send_over_transport(packets)
end
```

Packets arriving from that transport go back into the stack individually or in
a configured bounded batch:

```elixir
:ok = SmolNet.ingress(stack, complete_ip_packet)
{:ok, 2} = SmolNet.ingress(stack, [first_ip_packet, second_ip_packet])
```

This boundary carries IP packets, not Ethernet frames. The link owns transport
backpressure. When it terminates, the stack follows its configured `:link_down`
policy. `SmolNet.stop_stack/1` stops the complete temporary supervision bundle.

A link that should not outlive its stack monitors it. `SmolNet.monitor/1`
returns an ordinary monitor, and the link receives a standard `:DOWN` message
when the stack stops, whether through `SmolNet.stop_stack/1` or a crash:

```elixir
monitor = SmolNet.monitor(stack)

receive do
  {:DOWN, ^monitor, :process, _object, _reason} -> exit(:stack_down)
end
```

For a self-contained stack with no external transport, use
`SmolNet.Loopback`. It feeds every emitted packet back into the same stack:

```elixir
{:ok, _link, stack} =
  SmolNet.Loopback.start_link(
    addresses: [{{127, 0, 0, 1}, 8}, {{0, 0, 0, 0, 0, 0, 0, 1}, 128}]
  )
```

Run the complete loopback TCP example from a source checkout with:

```console
mix run examples/loopback.exs
```

## Choosing an API

SmolNet provides three interfaces over the same stack and socket machinery:

| Interface | Use it when | Guide |
| --- | --- | --- |
| `:gen_tcp` and `:inet` | Existing code expects OTP TCP sockets, active mode, or packet framing | [Using `:gen_tcp`](gen_tcp.md) |
| `:gen_udp` and `:inet` | Existing code expects OTP UDP sockets and active or passive delivery | [Using `:gen_udp`](gen_udp.md) |
| `SmolNet` | You want explicit endpoint maps, direct timeouts, or nonblocking readiness | [Using the low-level socket API](socket_api.md) |

Both address families are supported. The OTP adapters use `SmolNet.Inet.Tcp`
and `SmolNet.Inet.Udp` for IPv4, and `SmolNet.Inet6.Tcp` and
`SmolNet.Inet6.Udp` for IPv6. The low-level API selects `:inet` or `:inet6`
when a socket is opened.

## Design and limits

Each stack has one Elixir owner process and one private native stack. The owner
serializes access to the native resource; temporary supervised adapter
processes provide `:gen_tcp` and `:gen_udp` semantics. A failed stack or adapter
cannot corrupt another stack bundle.

Native calls never wait for network traffic. Work is bounded by a time budget
and deterministic limits on copied bytes, emitted packets, readiness events,
and maintenance. Blocking socket operations wait and retry in the calling
Elixir process. `SmolNet.stack_info/1` exposes lifecycle, socket, queue, and
work-budget telemetry.

The API is intentionally smaller than the host socket API. TCP supports raw,
line, and 1/2/4-byte length-prefixed packet modes. UDP preserves datagram
boundaries. Ancillary data, multicast, broadcast, OS file descriptors,
IPv4-mapped IPv6 addresses, and fragmented IPv4 ingress are not supported.

## Troubleshooting

- If ingress fails, supply a complete IPv4 or IPv6 packet within the configured
  MTU, or a list within the stack's `input_packets` and `bytes_copied` limits.
  Do not include Ethernet headers.
- If an operation times out, confirm that the link is forwarding outbound
  packets and returning peer traffic. SmolNet drives protocol timers, but it
  cannot move packets across the external transport.
- If a Hex dependency has no precompiled NIF for the host, use a source
  checkout on that target.
- Inspect `SmolNet.stack_info/1` before reporting a failure; it includes link,
  socket, queue, timer, and native-work diagnostics.

## License

SmolNet is released under the
[MIT License](https://github.com/ausimian/smolnet/blob/main/LICENSE).
