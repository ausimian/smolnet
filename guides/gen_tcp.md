# Using `:gen_tcp`

SmolNet can back Erlang's `:gen_tcp` and `:inet` APIs. This is the best
interface for code that already expects OTP socket conventions such as active
mode, packet framing, controlling-process ownership, and OTP-style error atoms.

## Select the SmolNet backend

Every socket needs two SmolNet-specific options:

- `{:tcp_module, module}` selects the callback for its address family.
- `{:smolnet_stack, stack}` selects the independent stack that owns it.

Use matching callback and family options:

| Family | Callback | Family option |
| --- | --- | --- |
| IPv4 | `SmolNet.InetBackend.Tcp4` | `:inet` |
| IPv6 | `SmolNet.InetBackend.Tcp` | `:inet6` |

For example, these are passive binary IPv4 options:

```elixir
options = [
  {:tcp_module, SmolNet.InetBackend.Tcp4},
  {:smolnet_stack, stack},
  :inet,
  :binary,
  {:active, false},
  {:packet, :raw}
]
```

The callback option affects only sockets created with that option list. It does
not replace the node-wide inet backend.

## A complete loopback example

`SmolNet.Loopback` is useful for examples and tests because it sends the
stack's packets straight back to the same stack:

```elixir
{:ok, link} =
  SmolNet.Loopback.start_link(addresses: [{{127, 0, 0, 1}, 8}])

stack = SmolNet.Loopback.stack(link)

options = [
  {:tcp_module, SmolNet.InetBackend.Tcp4},
  {:smolnet_stack, stack},
  :inet,
  :binary,
  {:active, false},
  {:ip, {127, 0, 0, 1}}
]

{:ok, listener} = :gen_tcp.listen(8080, options)

server =
  Task.async(fn ->
    {:ok, socket} = :gen_tcp.accept(listener, 5_000)
    {:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
    :ok = :gen_tcp.send(socket, ["echo: ", request])
    :ok = :gen_tcp.close(socket)
  end)

{:ok, client} =
  :gen_tcp.connect({127, 0, 0, 1}, 8080, options, 5_000)

:ok = :gen_tcp.send(client, "hello")
{:ok, "echo: hello"} = :gen_tcp.recv(client, 0, 5_000)

:ok = Task.await(server, 5_000)
:ok = :gen_tcp.close(client)
:ok = :gen_tcp.close(listener)
:ok = SmolNet.stop_stack(stack)
```

The same example is available as `examples/loopback.exs` in a source checkout.

## Clients

Connect with the normal `:gen_tcp` API. The target address must match the
selected family:

```elixir
ipv6_options = [
  {:tcp_module, SmolNet.InetBackend.Tcp},
  {:smolnet_stack, stack},
  :inet6,
  :binary,
  {:active, false}
]

peer = {0xFD00, 0, 0, 0, 0, 0, 0, 2}

{:ok, socket} = :gen_tcp.connect(peer, 443, ipv6_options, 5_000)
:ok = :gen_tcp.send(socket, "request")
{:ok, response} = :gen_tcp.recv(socket, 0, 5_000)
:ok = :gen_tcp.shutdown(socket, :write)
:ok = :gen_tcp.close(socket)
```

The stack must have a route to the peer, and the application's link must carry
the emitted packets to that route. A finite connect or receive timeout is
measured in the calling process; the stack owner and native scheduler never
block waiting for traffic.

## Servers

Listening and accepting use the standard calls:

```elixir
server_options = [{:backlog, 16} | ipv6_options]

{:ok, listener} = :gen_tcp.listen(8080, server_options)
{:ok, socket} = :gen_tcp.accept(listener, 5_000)
{:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
:ok = :gen_tcp.send(socket, request)
```

The backlog defaults to 5 and may be set from 1 through 128. Internally, a
listener maintains a bounded pool of native listening sockets. Accepted
sockets inherit the listener's mode, active setting, packet framing, buffer
sizes, and send-timeout policy, but have independent adapter state.

Closing a listener aborts a pending accept and releases children still waiting
in its accepted queue. Children already returned by `:gen_tcp.accept/2` remain
usable.

## Passive and active delivery

With `{:active, false}`, call `:gen_tcp.recv/3`. In raw mode, a length of zero
returns one currently available chunk. A positive length accumulates until
that many bytes arrive, the peer closes, or the operation fails:

```elixir
{:ok, chunk} = :gen_tcp.recv(socket, 0, 5_000)
{:ok, exactly_32_bytes} = :gen_tcp.recv(socket, 32, 5_000)
```

Active mode supports `true`, `:once`, and counts from 1 through 32,767:

```elixir
:ok = :inet.setopts(socket, active: :once)

receive do
  {:tcp, ^socket, data} -> handle_data(data)
  {:tcp_closed, ^socket} -> handle_close()
  {:tcp_error, ^socket, reason} -> handle_error(reason)
end
```

Counted mode sends `{:tcp_passive, socket}` when its count is exhausted. Active
delivery is bounded to 16 native reads or complete logical packets per adapter
mailbox turn, so a busy socket yields to other processes.

Only the controlling process receives active messages. Transfer ownership with
`:gen_tcp.controlling_process/2`; queued matching messages and future delivery
move to the new owner in order. If the owner exits, the adapter and its
low-level socket close.

## Packet framing and buffers

Supported packet modes are `:raw`, `:line`, `1`, `2`, and `4`. The integer
modes add or consume an unsigned big-endian length header of that width.
`packet_size` bounds a logical framed packet and defaults to 65,536 bytes.
Oversized framed input returns `:emsgsize` and closes the socket because the
stream can no longer be resynchronized.

Raw streams are not message-oriented: `packet_size` does not cap a raw send or
an exact-length raw receive. Large operations are advanced through bounded
native reads and writes while the adapter retains their progress.

There are two layers of buffer configuration:

- `buffer` is the adapter's Elixir-side receive buffer.
- `recbuf` and `sndbuf` are the native TCP receive and transmit capacities.

All three default to 65,536 bytes and accept values up to 1 MiB. Native
`recbuf` and `sndbuf` have a 1 KiB minimum and are fixed when the socket is
created; attempts to change them with `:inet.setopts/2` return `:einval`.
Setting `recbuf` at creation also raises `buffer` to at least that size unless
a later option explicitly lowers `buffer`.

`send_timeout` and `send_timeout_close` control a blocked adapter send. One
read and one write may be in progress concurrently; a second operation in the
same direction returns `:busy`.

## Supported options

The inet option surface is deliberately finite:

| Option | At connect/listen | At runtime |
| --- | --- | --- |
| `:smolnet_stack` | required | fixed |
| `:inet` / `:inet6` | supported | fixed |
| `:binary` / `:list` / `:mode` | supported | supported |
| `:active` | `false`, `true`, `:once`, or `1..32767` | supported |
| `:packet` | `:raw`, `:line`, `1`, `2`, or `4` | supported |
| `:packet_size` | `0..1048576` | supported |
| `:buffer` | `1..1048576` | supported |
| `:recbuf` / `:sndbuf` | `1024..1048576` | fixed |
| `:send_timeout` / `:send_timeout_close` | supported | supported |
| `:ip` / `:ifaddr` / `:port` | supported | fixed |
| `:backlog` | listen only, `1..128` | fixed |
| `:ipv6_v6only` | IPv6 `true` only | fixed |

Invalid values, options used in the wrong lifecycle phase, and unlisted
options return `:einval`. Selecting the wrong family returns `:eafnosupport`.
Ancillary data, OS file descriptors, raw socket options, and other packet modes
are not supported.

Network and lifecycle errors use the usual OTP-style atoms, including
`:econnrefused`, `:econnreset`, `:etimedout`, `:enetunreach`, `:eaddrinuse`,
`:closed`, and `:enetdown`.
