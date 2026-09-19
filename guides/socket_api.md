# Using the low-level socket API

The `SmolNet` module exposes a small, `:socket`-style API directly over an
embedded stack. It uses explicit address maps, timeout arguments, and one-shot
readiness notifications instead of an adapter process. It is similar in shape
to Erlang's `:socket` API, but it is not a drop-in implementation of that
module.

Choose this interface when the application wants precise control over socket
state and nonblocking retries. Choose the `:gen_tcp` or `:gen_udp` adapters
when existing code expects inet options, active messages, packet framing, or
controlling-process transfers.

## Create a stack and link

A stack owns its addresses, routes, sockets, protocol timers, and native
resource. Its link carries complete raw IP packets to and from an
application-defined transport:

```elixir
local = {0xFD00, 0, 0, 0, 0, 0, 0, 1}
gateway = {0xFD00, 0, 0, 0, 0, 0, 0, 0xFF}

{:ok, stack} =
  SmolNet.start_stack(
    egress: {self(), :tunnel},
    mtu: 1280,
    addresses: [{local, 64}],
    routes: [{{0, 0, 0, 0, 0, 0, 0, 0}, 0, gateway}],
    link_down: :stop
  )
```

The egress recipient receives non-empty, bounded batches in packet order:

```elixir
{:smol_stack, :tunnel, :egress, [complete_ip_packet, ...]}
```

Feed one complete IPv4 or IPv6 packet back with:

```elixir
:ok = SmolNet.ingress(stack, complete_ip_packet)
```

Ingress accepts raw IP packets, not Ethernet frames. IPv4 input is validated
for header length, total length, checksum, MTU, and fragmentation before it
mutates the stack. Fragmented IPv4 input is rejected; reassembly belongs
outside this API.

Each stack has one serialized link feeder. The link is responsible for
transport-level buffering and backpressure. If a bounded native continuation
still owns the ingress slot, a feeder can receive `{:error, :busy}` and should
retry after yielding. The `:link_down` policy may be `:stop`, `:mark_down`, or
`{:notify, pid}`.

For examples and tests, `SmolNet.Loopback` supplies a link that feeds every
emitted packet back into the same stack:

```elixir
{:ok, link} =
  SmolNet.Loopback.start_link(addresses: [{{127, 0, 0, 1}, 8}])

stack = SmolNet.Loopback.stack(link)
```

## Endpoint maps

IPv4 and IPv6 are explicit:

```elixir
ipv4 = %{family: :inet, addr: {192, 0, 2, 2}, port: 443}

ipv6 = %{
  family: :inet6,
  addr: {0xFD00, 0, 0, 0, 0, 0, 0, 2},
  port: 443
}
```

IPv6 maps may also contain `flowinfo: 0` and `scope_id: 0`. A link-local
`fe80::/10` endpoint requires a positive `scope_id`; a global address uses
scope zero. IPv4-mapped IPv6 addresses are intentionally unsupported.

A socket's family is fixed by `SmolNet.open/4`. Passing an endpoint from the
other family returns an error rather than converting it.

## TCP clients

Open, optionally bind, and connect a stream socket:

```elixir
remote = {0xFD00, 0, 0, 0, 0, 0, 0, 2}

{:ok, socket} =
  SmolNet.open(:inet6, :stream, :tcp,
    stack: stack,
    rcvbuf: 192 * 1024,
    sndbuf: 192 * 1024
  )

:ok =
  SmolNet.bind(socket, %{
    family: :inet6,
    addr: local,
    port: 0
  })

peer = %{family: :inet6, addr: remote, port: 443}

:ok = SmolNet.connect(socket, peer, 5_000)
:ok = SmolNet.send(socket, ["hello", " world"], 5_000)
{:ok, response} = SmolNet.recv(socket, 128, 5_000)

{:ok, local_endpoint} = SmolNet.sockname(socket)
{:ok, %{family: :inet6, addr: ^remote, port: 443}} =
  SmolNet.peername(socket)

:ok = SmolNet.shutdown(socket, :write)
:ok = SmolNet.close(socket)
```

TCP receive and transmit buffers default to 64 KiB. `rcvbuf` and `sndbuf`
accept sizes from 1 KiB through 1 MiB and cannot be resized after opening.

`recv(socket, 0, timeout)` returns one bounded currently available chunk. A
positive synchronous length accumulates bounded reads until it has exactly
that many bytes, the operation fails, or the peer reaches EOF. EOF returns
buffered data first; the next receive returns `{:error, :closed}`. If a timeout
or error follows partial progress, receive returns the accumulated bytes with
the reason; send similarly returns its unsent remainder.

## TCP listeners

A bound stream socket becomes a listener with a backlog from 1 through 128:

```elixir
{:ok, listener} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

:ok =
  SmolNet.bind(listener, %{
    family: :inet6,
    addr: local,
    port: 8080
  })

:ok = SmolNet.listen(listener, 16)

{:ok, child} = SmolNet.accept(listener, 5_000)
{:ok, request} = SmolNet.recv(child, 0, 5_000)
:ok = SmolNet.send(child, request, 5_000)
```

Each accepted child has a new public identity and becomes independent of its
listener. A listener maintains up to four native listening sockets and an
accepted queue no larger than its requested backlog. Closing the listener
aborts a pending accept and releases queued children; children already returned
to callers remain usable.

Wildcard listeners remain restricted to the family chosen at open, even on a
dual-family stack.

## UDP sockets

Open datagram sockets with `:dgram` and `:udp`:

```elixir
local_endpoint = %{family: :inet6, addr: local, port: 0}
peer_endpoint = %{family: :inet6, addr: remote, port: 53}

{:ok, socket} = SmolNet.open(:inet6, :dgram, :udp, stack: stack)
:ok = SmolNet.bind(socket, local_endpoint)
:ok = SmolNet.sendto(socket, <<0, 1, "query">>, peer_endpoint, 5_000)

{:ok, datagram} = SmolNet.recvfrom(socket, 0, 5_000)

%{
  source: source,
  destination: destination,
  data: payload,
  truncated: false
} = datagram
```

A zero receive length returns one complete datagram, including a zero-length
datagram. A positive length returns at most that many bytes, discards the rest
of the same datagram, and sets `truncated: true`. Both the source endpoint and
the packet's actual local destination are retained.

Each native UDP socket has rings for 16 packet descriptors and 16 KiB of
payload. A datagram payload may not exceed the smaller of 16,384 bytes and the
stack MTU minus 28 bytes for IPv4 or 48 bytes for IPv6. A send is accepted in
full or not at all.

`SmolNet.connect/2` records a default UDP peer. Later sends are restricted to
that peer, and datagrams from other peers are discarded. TCP and UDP have
independent port namespaces, and IPv4 and IPv6 are also isolated, so all four
combinations can bind the same numeric port on one stack.

## Nonblocking operations

Pass `:nowait` to connect, accept, send, receive, or datagram operations. When
the operation would block, it returns a one-shot select value:

```elixir
{:select, {:select_info, _operation, reference} = select_info} =
  SmolNet.recv(socket, 0, :nowait)

socket_identity = {socket.id, socket.generation}

receive do
  {:"$smol_socket", ^socket_identity, :select, ^reference} ->
    SmolNet.recv(socket, 0, :nowait)

  {:"$smol_socket", ^socket_identity, :abort, ^reference, reason} ->
    {:error, reason}
end
```

The message is only a retry hint; readiness can change before the retry. The
select is one-shot, so each blocked retry returns a new select value. Closing
the socket or stack can send the corresponding `:abort` message instead.

Stream operations retain partial progress in the caller:

```elixir
{:select, {send_info, unsent}} =
  SmolNet.send(socket, large_binary, :nowait)

{:select, {recv_info, partial}} =
  SmolNet.recv(socket, exact_length, :nowait)
```

After the matching notification, retry only `unsent`, or request the remaining
receive length while retaining `partial` in the caller. UDP never returns
partial send progress; retry the original complete datagram.

Cancel a waiter with its exact select value:

```elixir
case SmolNet.cancel(socket, select_info) do
  :ok -> :cancelled
  :already_sent -> :notification_won_the_race
  :not_found -> :no_matching_waiter
  {:error, reason} -> {:socket_unavailable, reason}
end
```

One read-direction and one write-direction waiter can coexist. A competing
operation in the same direction returns `:busy`.

## Resource and error contracts

`%SmolNet.Socket{}` values are lightweight identities, not processes. Socket
IDs and generations are never reused. Closing a socket invalidates its public
handle immediately; a TCP closing record can remain temporarily to drive FIN
and retransmission.

A stack has bounded socket, packet, waiter, readiness, and maintenance state.
The native socket limit is 64 backing sockets per stack, including listener
pools and wildcard UDP expansion. `SmolNet.stack_info/1` reports live usage,
buffer capacities, queue metrics, and work-budget measurements.

Public calls return `:ok`, `{:ok, value}`, `{:select, continuation}`, or
`{:error, reason}`. Stable errors distinguish validation, lifecycle, and
network failures. Common examples include `:invalid_options`,
`:invalid_address`, `:message_too_large`, `:network_unreachable`,
`:connection_refused`, `:connection_reset`, `:connection_timeout`, `:busy`,
`:closed`, and `:invalid_socket`. Treat each select notification only as
permission to retry.
